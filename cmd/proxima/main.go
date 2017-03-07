package main

import (
	"flag"
	"github.com/Symantec/Dominator/lib/flagutil"
	"github.com/Symantec/Dominator/lib/fsutil"
	"github.com/Symantec/Dominator/lib/logbuf"
	"github.com/Symantec/proxima/cmd/proxima/splash"
	"github.com/Symantec/scotty/influx/responses"
	"github.com/Symantec/scotty/lib/apiutil"
	"github.com/Symantec/tricorder/go/healthserver"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/uuid"
	"log"
	"net/http"
	"net/rpc"
	"net/url"
	"strings"
	"time"
)

var (
	fConfigFile = flag.String(
		"config", "/etc/proxima/proxima.yaml", "config file")
	fPorts = flagutil.StringList{"8086"}
)

func init() {
	flag.Var(&fPorts, "ports", "Comma separated list of ports")
}

func setHeader(w http.ResponseWriter, r *http.Request, key, value string) {
	r.Header.Set(key, value)
	w.Header().Set(key, value)
}

func uuidHandler(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid := uuid.TimeUUID()
		setHeader(w, r, "Request-Id", uid.String())
		setHeader(w, r, "X-Influxdb-Version", "0.13.0")
		inner.ServeHTTP(w, r)
	})
}

type seriesListType struct {
	Series []models.Row `json:"series"`
}

type resultListType struct {
	Results []seriesListType `json:"results"`
}

func performQuery(
	executer *executerType,
	logger *log.Logger,
	query, db, epoch string) (interface{}, error) {
	switch strings.ToUpper(query) {
	case "SHOW MEASUREMENTS LIMIT 1":
		return responses.Serialise(&client.Response{
			Results: []client.Result{
				{
					Series: []models.Row{
						{
							Name:    "measurements",
							Columns: []string{"name"},
							Values:  [][]interface{}{{"aname"}},
						},
					},
				},
			},
		})
	case "SHOW DATABASES":
		dbNames := executer.Names()
		values := make([][]interface{}, len(dbNames))
		for i := range dbNames {
			values[i] = []interface{}{dbNames[i]}
		}
		return responses.Serialise(&client.Response{
			Results: []client.Result{
				{
					Series: []models.Row{
						{
							Name:    "databases",
							Columns: []string{"name"},
							Values:  values,
						},
					},
				},
			},
		})
	default:
		resp, err := executer.Query(logger, query, db, epoch)
		if err != nil {
			return nil, err
		}
		return responses.Serialise(resp)
	}
}

func dateHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setHeader(w, r, "Date", time.Now().UTC().Format("Mon, 2 Jan 2006 15:04:05 MST"))
		w.WriteHeader(204)
	})

}

func main() {
	tricorder.RegisterFlags()
	flag.Parse()
	rpc.HandleHTTP()
	circularBuffer := logbuf.New()
	logger := log.New(circularBuffer, "", log.LstdFlags)
	executer := newExecuter()
	changeCh := fsutil.WatchFile(*fConfigFile, logger)
	// We want to be sure we have something valid in the config file
	// initially.
	select {
	case readCloser := <-changeCh:
		if err := executer.SetupWithStream(readCloser); err != nil {
			logger.Println(err)
		}
		readCloser.Close()
	case <-time.After(time.Second):
		logger.Println("No config file initially")
	}
	go func() {
		for readCloser := range changeCh {
			if err := executer.SetupWithStream(readCloser); err != nil {
				logger.Println(err)
			}
			readCloser.Close()
		}
	}()
	http.Handle("/",
		&splash.Handler{
			Log: circularBuffer,
		})
	http.Handle(
		"/ping",
		uuidHandler(dateHandler()),
	)
	http.Handle(
		"/query",
		uuidHandler(
			apiutil.NewHandler(
				func(req url.Values) (interface{}, error) {
					resp, err := performQuery(
						executer,
						logger,
						req.Get("q"),
						req.Get("db"),
						req.Get("epoch"))
					if err != nil {
						return nil, err
					}
					return resp, err
				},
				nil,
			),
		),
	)
	if len(fPorts) == 0 {
		log.Fatal("At least one port required.")
	}
	for _, port := range fPorts[1:] {
		go func(port string) {
			if err := http.ListenAndServe(":"+port, nil); err != nil {
				log.Fatal(err)
			}
		}(port)
	}
	healthserver.SetReady()
	if err := http.ListenAndServe(":"+fPorts[0], nil); err != nil {
		log.Fatal(err)
	}
}
