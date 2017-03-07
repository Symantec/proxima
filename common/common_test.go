package common

import (
	"encoding/json"
	"errors"
	"github.com/Symantec/proxima/config"
	"github.com/Symantec/scotty/influx/qlutils"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	. "github.com/smartystreets/goconvey/convey"
	"strconv"
	"testing"
	"time"
)

var (
	kErrCreatingHandle = errors.New("common:Error creating handle")
	kErrSomeError      = errors.New("common:some error")
)

type queryCallType struct {
	query    string
	database string
	epoch    string
}

// fakeHandleType represents a connection to a fake influx backend or
// scotty server.
type fakeHandleType struct {
	queryCalls    []queryCallType
	queryResponse *client.Response
	queryError    error
	closed        bool
}

// WhenQueriedReturn instructs this fake to return a particular response
// or error when queried. This fake always returns this same response
// regardless of the actual query.
func (f *fakeHandleType) WhenQueriedReturn(
	response *client.Response, err error) {
	f.queryResponse, f.queryError = response, err
}

// NextQuery returns the next query this fake received.
// A query consists of three parts, the query string, the influx database,
// and the epoch, the precision of the times e.g "ns", "ms", "s", etc.
// If previous NextQuery calls have already returned all the queries made
// against this fake, NextQuery panics.
func (f *fakeHandleType) NextQuery() (
	query, database, epoch string) {
	query = f.queryCalls[0].query
	database = f.queryCalls[0].database
	epoch = f.queryCalls[0].epoch
	length := len(f.queryCalls)
	copy(f.queryCalls, f.queryCalls[1:])
	f.queryCalls = f.queryCalls[:length-1]
	return
}

// NoMoreQueries returns true if NextQuery would panic.
func (f *fakeHandleType) NoMoreQueries() bool {
	return len(f.queryCalls) == 0
}

// Closed returns true if Close was called on this fake
func (f *fakeHandleType) Closed() bool {
	return f.closed
}

// Query sends a query to the fake influx or scotty server, records the
// query sent, and returns the same response and error passed to
// WhenQueriedReturn.
func (f *fakeHandleType) Query(queryStr, database, epoch string) (
	*client.Response, error) {
	if f.closed {
		panic("Cannot query a closed handle")
	}
	f.queryCalls = append(
		f.queryCalls,
		queryCallType{
			query:    queryStr,
			database: database,
			epoch:    epoch,
		})
	return f.queryResponse, f.queryError
}

// Close closes the connection to the fake influx or scotty server.
func (f *fakeHandleType) Close() error {
	f.closed = true
	return nil
}

// handleStoreType is a collection of fake influx backends and scotty servers
// keyed by their host and port.
type handleStoreType map[string]*fakeHandleType

// Create returns the connection to the fake server given its host and port.
func (s handleStoreType) Create(addr string) (handleType, error) {
	result, ok := s[addr]
	if !ok {
		return nil, kErrCreatingHandle
	}
	return result, nil
}

// AllClose returns true if all connections to all fakes have been closed.
func (s handleStoreType) AllClosed() bool {
	for _, h := range s {
		if !h.Closed() {
			return false
		}
	}
	return true
}

var (
	kTimeValueColumns = []string{"time", "value"}
)

func newResponse(values ...int64) *client.Response {
	realValues := make([][]interface{}, len(values)/2)
	for i := range realValues {
		realValues[i] = []interface{}{
			json.Number(strconv.FormatInt(values[2*i], 10)),
			json.Number(strconv.FormatInt(values[2*i+1], 10)),
		}
	}
	return &client.Response{
		Results: []client.Result{
			{
				Series: []models.Row{
					{
						Name:    "alpha",
						Columns: kTimeValueColumns,
						Values:  realValues,
					},
				},
			},
		},
	}

}

func TestAPI(t *testing.T) {
	Convey("Given fake sources", t, func() {
		now := time.Date(2016, 12, 1, 0, 1, 0, 0, time.UTC)
		// All of our fake servers / backends
		store := handleStoreType{
			"alpha":       &fakeHandleType{},
			"bravo":       &fakeHandleType{},
			"charlie":     &fakeHandleType{},
			"delta":       &fakeHandleType{},
			"echo":        &fakeHandleType{},
			"foxtrot":     &fakeHandleType{},
			"error":       &fakeHandleType{},
			"error1":      &fakeHandleType{},
			"unsupported": &fakeHandleType{},
		}
		// These lines tell each fake what to return when queried.
		store["alpha"].WhenQueriedReturn(newResponse(1000, 10, 1200, 11), nil)
		store["bravo"].WhenQueriedReturn(newResponse(1200, 12, 1400, 13), nil)
		store["charlie"].WhenQueriedReturn(newResponse(1400, 14, 1600, 15), nil)
		store["delta"].WhenQueriedReturn(newResponse(1400, 24, 1600, 25), nil)
		store["echo"].WhenQueriedReturn(newResponse(1600, 26, 1800, 27), nil)
		store["foxtrot"].WhenQueriedReturn(newResponse(1800, 28, 2000, 29), nil)
		store["error"].WhenQueriedReturn(nil, kErrSomeError)
		store["error1"].WhenQueriedReturn(&client.Response{Err: kErrSomeError.Error()}, nil)
		store["unsupported"].WhenQueriedReturn(&client.Response{Err: qlutils.ErrUnsupported.Error()}, nil)

		Convey("A proxima config with duplicate db names", func() {
			proximaConfig := config.Proxima{
				Dbs: []config.Database{
					{
						Name: "influx",
					},
					{
						Name: "nothing",
					},
					{
						Name: "influx",
					},
				},
			}
			Convey("Should produce an error", func() {
				_, err := NewProxima(proximaConfig)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("Given a valid proxima config", func() {
			influxConfigs := config.InfluxList{
				{
					HostAndPort: "charlie",
					Database:    "c",
					Duration:    time.Hour,
				},
				{
					HostAndPort: "alpha",
					Database:    "a",
					Duration:    100 * time.Hour,
				},
				{
					HostAndPort: "bravo",
					Database:    "b",
					Duration:    10 * time.Hour,
				},
				{
					HostAndPort: "error",
					Database:    "err",
					Duration:    50 * time.Hour,
				},
				{
					HostAndPort: "error1",
					Database:    "err1",
					Duration:    20 * time.Hour,
				},
			}

			scottyConfigs := config.ScottyList{
				{HostAndPort: "delta"},
				{HostAndPort: "echo"},
				{HostAndPort: "foxtrot"},
				{HostAndPort: "error"},
				{HostAndPort: "error1"},
			}

			unsupportedInScotty := config.ScottyList{
				{HostAndPort: "unsupported"},
			}

			proximaConfig := config.Proxima{
				Dbs: []config.Database{
					{
						Name:     "influx",
						Influxes: influxConfigs,
					},
					{
						Name:     "unsupportedButInflux",
						Influxes: influxConfigs,
						Scotties: unsupportedInScotty,
					},
					{
						Name:     "unsupported",
						Scotties: unsupportedInScotty,
					},
					{
						Name:     "scotty",
						Scotties: scottyConfigs,
					},
					{
						Name: "nothing",
					},
					{
						Name:     "both",
						Influxes: influxConfigs,
						Scotties: scottyConfigs,
					},
				},
			}
			// Create a proxima instance that uses our fakes rather than
			// connecting to real influx backends and scotty servers.
			proxima, err := newProximaForTesting(proximaConfig, store.Create)
			Convey("Close should free resources", func() {
				So(proxima.Close(), ShouldBeNil)
				So(store.AllClosed(), ShouldBeTrue)
			})
			Convey("Names should return names in alphabetical order", func() {
				names := proxima.Names()
				So(names, ShouldResemble, []string{
					"both",
					"influx",
					"nothing",
					"scotty",
					"unsupported",
					"unsupportedButInflux"})
			})
			So(err, ShouldBeNil)
			Convey("Nothing", func() {
				db := proxima.ByName("nothing")
				So(db, ShouldNotBeNil)
				Convey("Query should return zero", func() {
					query, err := qlutils.NewQuery(
						"select mean(value) from dual where time >= now() - 5h", now)
					So(err, ShouldBeNil)
					response, err := db.Query(nil, query, "ns", now)
					So(err, ShouldBeNil)
					So(*response, ShouldBeZeroValue)
				})
			})
			Convey("Unsupported in scotty but influx", func() {
				db := proxima.ByName("unsupportedButInflux")
				So(db, ShouldNotBeNil)
				Convey("Should fall back to influx", func() {
					query, err := qlutils.NewQuery(
						"select mean(value) from dual where time >= now() - 5h", now)
					So(err, ShouldBeNil)
					response, err := db.Query(nil, query, "ms", now)
					So(err, ShouldBeNil)
					// In the case that scotty doesn't support the query,
					// rely on the influx servers.
					So(response, ShouldResemble, newResponse(
						1000, 10,
						1200, 12,
						1400, 14,
						1600, 15,
					))
				})
			})
			Convey("Just influx", func() {
				db := proxima.ByName("influx")
				So(db, ShouldNotBeNil)

				Convey("Query going to now should work", func() {
					query, err := qlutils.NewQuery(
						"select mean(value) from dual where time >= now() - 5h", now)
					So(err, ShouldBeNil)
					response, err := db.Query(nil, query, "ns", now)
					So(err, ShouldBeNil)
					// influx backend with shortest retention policy always
					// takes precedence.
					So(response, ShouldResemble, newResponse(
						1000, 10,
						1200, 12,
						1400, 14,
						1600, 15,
					))
					// 'alpha' which has 100h retention policy should receive
					// the original query.
					queryStr, database, epoch := store["alpha"].NextQuery()
					So(
						queryStr,
						ShouldEqual,
						"SELECT mean(value) FROM dual WHERE time >= '2016-11-30T19:01:00Z' AND time < '2016-12-01T00:01:00Z'")
					So(database, ShouldEqual, "a")
					So(epoch, ShouldEqual, "ns")
					So(store["alpha"].NoMoreQueries(), ShouldBeTrue)

					// 'bravo' which has 10h retention policy should receive
					// the original query.
					queryStr, database, epoch = store["bravo"].NextQuery()
					So(
						queryStr,
						ShouldEqual,
						"SELECT mean(value) FROM dual WHERE time >= '2016-11-30T19:01:00Z' AND time < '2016-12-01T00:01:00Z'")
					So(database, ShouldEqual, "b")
					So(epoch, ShouldEqual, "ns")
					So(store["bravo"].NoMoreQueries(), ShouldBeTrue)

					// 'charlie' which has 1h retention policy should receive
					// the query modified to go back only one hour
					queryStr, database, epoch = store["charlie"].NextQuery()
					So(
						queryStr,
						ShouldEqual,
						"SELECT mean(value) FROM dual WHERE time >= '2016-11-30T23:01:00Z' AND time < '2016-12-01T00:01:00Z'")
					So(database, ShouldEqual, "c")
					So(epoch, ShouldEqual, "ns")
					So(store["charlie"].NoMoreQueries(), ShouldBeTrue)
				})

				Convey("query stopping before now should work", func() {
					query, err := qlutils.NewQuery(
						"select mean(value) from dual where time >= now() - 120h and time < now() - 5h", now)
					So(err, ShouldBeNil)
					response, err := db.Query(nil, query, "ns", now)
					So(err, ShouldBeNil)
					So(response, ShouldResemble, newResponse(
						1000, 10,
						1200, 12,
						1400, 13,
					))
					// 'alpha' with a 100 hour retention policy should receive
					// original query modified to go back only 100 hours.
					queryStr, database, epoch := store["alpha"].NextQuery()
					So(
						queryStr,
						ShouldEqual,
						"SELECT mean(value) FROM dual WHERE time >= '2016-11-26T20:01:00Z' AND time < '2016-11-30T19:01:00Z'")
					So(database, ShouldEqual, "a")
					So(epoch, ShouldEqual, "ns")
					So(store["alpha"].NoMoreQueries(), ShouldBeTrue)

					// 'bravo' with a 10 hour retention policy should receive
					// original query modified to go back only 10 hours.
					queryStr, database, epoch = store["bravo"].NextQuery()
					So(
						queryStr,
						ShouldEqual,
						"SELECT mean(value) FROM dual WHERE time >= '2016-11-30T14:01:00Z' AND time < '2016-11-30T19:01:00Z'")
					So(database, ShouldEqual, "b")
					So(epoch, ShouldEqual, "ns")
					So(store["bravo"].NoMoreQueries(), ShouldBeTrue)

					// 'charlie' should receive no queries since its retention
					// policy is 1h, and the query ends 5 hours back in time.
					So(store["charlie"].NoMoreQueries(), ShouldBeTrue)
				})
			})
			Convey("Just scotty", func() {
				db := proxima.ByName("scotty")
				So(db, ShouldNotBeNil)

				Convey("Scotty query should work", func() {
					query, err := qlutils.NewQuery(
						"select mean(value) from dual where time >= now() - 5h", now)
					So(err, ShouldBeNil)
					response, err := db.Query(nil, query, "ms", now)
					So(err, ShouldBeNil)
					// scotty server listed last takes precedence.
					So(response, ShouldResemble, newResponse(
						1400, 24,
						1600, 26,
						1800, 28,
						2000, 29,
					))
					// Unlike influx servers, original query sent to all
					// scotty servers.
					queryStr, database, epoch := store["delta"].NextQuery()
					So(
						queryStr,
						ShouldEqual,
						"SELECT mean(value) FROM dual WHERE time >= '2016-11-30T19:01:00Z'")
					So(database, ShouldEqual, "scotty")
					So(epoch, ShouldEqual, "ms")
					So(store["delta"].NoMoreQueries(), ShouldBeTrue)

					queryStr, database, epoch = store["echo"].NextQuery()
					So(
						queryStr,
						ShouldEqual,
						"SELECT mean(value) FROM dual WHERE time >= '2016-11-30T19:01:00Z'")
					So(database, ShouldEqual, "scotty")
					So(epoch, ShouldEqual, "ms")
					So(store["echo"].NoMoreQueries(), ShouldBeTrue)

					queryStr, database, epoch = store["foxtrot"].NextQuery()
					So(
						queryStr,
						ShouldEqual,
						"SELECT mean(value) FROM dual WHERE time >= '2016-11-30T19:01:00Z'")
					So(database, ShouldEqual, "scotty")
					So(epoch, ShouldEqual, "ms")
					So(store["foxtrot"].NoMoreQueries(), ShouldBeTrue)
				})
			})
		})
	})
}
