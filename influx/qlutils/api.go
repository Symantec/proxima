package qlutils

import (
	"errors"
	"github.com/influxdata/influxdb/influxql"
	"time"
)

var (
	// means the query contains a statement that is not a select statement.
	ErrNonSelectStatement = errors.New("qlutils: Non select statement")
)

// NewQuery creates a new query instance from a string substituting currentTime
// for now().
func NewQuery(ql string, currentTime time.Time) (*influxql.Query, error) {
	return newQuery(ql, currentTime)
}

// QueryTimeRange returns the min and max time for a query. If no min time
// found, min is the zero value; if no max time found, max = now.
func QueryTimeRange(
	query *influxql.Query, now time.Time) (min, max time.Time, err error) {
	return queryTimeRange(query, now)
}

// QuerySetTimeRange returns a query just like query except that it is only
// for times falling between min inclusive and max exclusive. If none of the
// select statements in the query matches the given time range,
// returns nil, nil
func QuerySetTimeRange(
	query *influxql.Query, min, max time.Time) (*influxql.Query, error) {
	return querySetTimeRange(query, min, max)
}
