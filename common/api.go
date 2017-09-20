// Package common does the heavy lifting for proxima.
package common

import (
	"github.com/Symantec/Dominator/lib/log"
	"github.com/Symantec/proxima/config"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"time"
)

// Influx represents a single influx backend.
type Influx struct {
	data      config.Influx
	dbQueryer dbQueryerType
}

func NewInflux(influx config.Influx) (*Influx, error) {
	return newInfluxForTesting(influx, influxCreateDbQueryer)
}

// Query runs a query against this backend.
func (d *Influx) Query(
	query *influxql.Query, epoch string, logger log.Logger) (
	*client.Response, error) {
	return d.dbQueryer.Query(query.String(), d.data.Database, epoch)
}

// Close frees any resources associated with this instance.
func (d *Influx) Close() error {
	return d.dbQueryer.Close()
}

// InfluxList represents a group of influx backends.
// nil represents the group of zero influx backends.
type InfluxList struct {
	instances []*Influx
}

// NewInfluxList returns a new instancce. If the length of influxes is 0,
// NewInfluxList returns nil.
func NewInfluxList(influxes config.InfluxList) (*InfluxList, error) {
	return newInfluxListForTesting(influxes, influxCreateDbQueryer)
}

// Query runs a query against the backends in this group merging the resuls
// into a single response.
func (l *InfluxList) Query(
	query *influxql.Query, epoch string, now time.Time, logger log.Logger) (
	*client.Response, error) {
	return l.query(query, epoch, now, logger)
}

// Close frees any resources associated with this instance.
func (l *InfluxList) Close() error {
	return l._close()
}

// Scotty represents a single scotty server.
type Scotty struct {
	// connects to a particular scotty. Only one of these fields will be
	// non nil
	dbQueryer dbQueryerType

	// Scotties which all together represent the data
	partials *ScottyPartials

	// Each scotty represents the same data.
	scotties *ScottyList
}

func NewScotty(scotty config.Scotty) (*Scotty, error) {
	return newScottyForTesting(scotty, influxCreateDbQueryer)
}

func (s *Scotty) Query(
	query *influxql.Query, epoch string, logger log.Logger) (
	*client.Response, error) {
	return s.query(query, epoch, logger)
}

// Close frees any resources associated with this instance.
func (s *Scotty) Close() error {
	return s._close()
}

// ScottyPartials represents a list of scotties where all the scotties
// together represent the data. All scotties must respond to each query.
type ScottyPartials struct {
	instances []*Scotty
}

func NewScottyPartials(scotties config.ScottyList) (*ScottyPartials, error) {
	return newScottyPartialsForTesting(scotties, influxCreateDbQueryer)
}

func (l *ScottyPartials) Query(
	query *influxql.Query, epoch string, logger log.Logger) (
	*client.Response, error) {
	return l.query(query, epoch, logger)
}

func (l *ScottyPartials) Close() error {
	return l._close()
}

// ScottyList represents a group of scotty servers. Unlike ScottyPartials,
// each Scotty has the same data only one scotty has to respond to each query.
// nil represents the group of zero scotty servers.
type ScottyList struct {
	instances []*Scotty
}

// NewScottyList returns a new instancce. If the length of scotties is 0,
// NewScottyList returns nil.
func NewScottyList(scotties config.ScottyList) (*ScottyList, error) {
	return newScottyListForTesting(scotties, influxCreateDbQueryer)
}

// Query runs a query against the servers in this group merging the resuls
// into a single response.
func (l *ScottyList) Query(
	query *influxql.Query, epoch string, logger log.Logger) (
	*client.Response, error) {
	return l.query(query, epoch, logger)
}

// Close frees any resources associated with this instance.
func (l *ScottyList) Close() error {
	return l._close()
}

// Database represents a single proxima configuration.
type Database struct {
	name     string
	influxes *InfluxList
	scotties *ScottyList
}

func NewDatabase(db config.Database) (*Database, error) {
	return newDatabaseForTesting(db, influxCreateDbQueryer)
}

func (d *Database) Name() string {
	return d.name
}

// Query runs a query against the influx backends and scotty servers in this
// proxima configuration.
func (d *Database) Query(
	query *influxql.Query,
	epoch string,
	now time.Time,
	logger log.Logger) (*client.Response, error) {
	return d.query(query, epoch, now, logger)
}

// Close frees any resources associated with this instance.
func (d *Database) Close() error {
	return d._close()
}

// Proxima represents all the configurations of a proxima application.
// A Proxima instance does the heavy lifting for the proxima application.
type Proxima struct {
	dbs map[string]*Database
}

func NewProxima(proxima config.Proxima) (*Proxima, error) {
	return newProximaForTesting(proxima, influxCreateDbQueryer)
}

// ByName returns the configuration with given name or nil if no such
// configuration exists.
func (p *Proxima) ByName(name string) *Database {
	return p.dbs[name]
}

// Names returns the names of all the configurations ordered alphabetically.
func (p *Proxima) Names() (result []string) {
	return p.names()
}

// Close frees any resources associated with this instance.
func (p *Proxima) Close() error {
	return p._close()
}
