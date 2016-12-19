package main

import (
	"flag"
	"github.com/Symantec/Dominator/lib/fsutil"
	"github.com/Symantec/scotty/lib/apiutil"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/uuid"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"
)

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

func main() {
	fConfigFile := flag.String("config", "./proxima.config", "config file")
	fPort := flag.String("port", ":8086", "listen port")
	flag.Parse()
	executer := newExecuter()
	logger := log.New(os.Stderr, "", log.LstdFlags)
	changeCh := fsutil.WatchFile(*fConfigFile, logger)
	// We want to be sure we have something valid in the config file
	// initially.
	select {
	case readCloser := <-changeCh:
		if err := executer.SetupWithStream(readCloser); err != nil {
			logger.Fatal(err)
		}
		readCloser.Close()
	case <-time.After(time.Second):
		logger.Fatal("No config file")
	}
	go func() {
		for readCloser := range changeCh {
			if err := executer.SetupWithStream(readCloser); err != nil {
				logger.Println(err)
			}
			readCloser.Close()
		}
	}()
	http.Handle(
		"/query",
		uuidHandler(
			apiutil.NewHandler(
				func(req url.Values) (interface{}, error) {
					resp, err := executer.Query(
						req.Get("q"),
						req.Get("db"),
						req.Get("epoch"))
					if err == nil {
						err = resp.Error()
					}
					if err != nil {
						return nil, err
					}
					results := &resultListType{
						Results: make([]seriesListType, len(resp.Results)),
					}
					for i := range results.Results {
						theSeries := resp.Results[i].Series
						if theSeries == nil {
							theSeries = []models.Row{}
						}
						results.Results[i] = seriesListType{
							Series: theSeries,
						}
					}
					return results, nil

				},
				nil,
			),
		),
	)
	if err := http.ListenAndServe(*fPort, nil); err != nil {
		log.Fatal(err)
	}
}
