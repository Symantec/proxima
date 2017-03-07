package main

import (
	"errors"
	"github.com/Symantec/proxima/common"
	"github.com/Symantec/proxima/config"
	"github.com/Symantec/scotty/influx/qlutils"
	"github.com/Symantec/scotty/lib/pool"
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"github.com/influxdata/influxdb/client/v2"
	"io"
	"log"
	"strconv"
	"time"
)

var (
	kDatabasesTricorderPath = "/proc/databases"
)

var (
	kErrNoSuchDatabase = errors.New("No such database.")
)

type proximaResourceType struct {
	*pool.SingleResource
}

func NewProximaResource(p *common.Proxima) *proximaResourceType {
	return &proximaResourceType{
		SingleResource: pool.NewSingleResource(p),
	}
}

func (p *proximaResourceType) Get() (uint64, *common.Proxima) {
	id, val := p.SingleResource.Get()
	return id, val.(*common.Proxima)
}

func (p *proximaResourceType) Set(proxima *common.Proxima) {
	p.SingleResource.Set(proxima)
}

// executerType executes queries across multiple influx db instances.
// executerType instances are safe to use with multiple goroutines
type executerType struct {
	proxima *proximaResourceType
}

// newExecuter returns a new instance with no configuration. Querying it
// will always yield errNoBackends.
func newExecuter() *executerType {
	proxima, err := common.NewProxima(config.Proxima{})
	if err != nil {
		panic(err)
	}
	return &executerType{proxima: NewProximaResource(proxima)}
}

// SetupWithStream sets up this instance with config file contents in r.
func (e *executerType) SetupWithStream(r io.Reader) error {
	var proximaConfig config.Proxima
	if err := yamlutil.Read(r, &proximaConfig); err != nil {
		return err
	}
	proxima, err := common.NewProxima(proximaConfig)
	if err != nil {
		return err
	}
	if err := registerProxima(proximaConfig); err != nil {
		return err
	}
	e.proxima.Set(proxima)
	return nil
}

func registerInflux(
	influx config.Influx, dir *tricorder.DirectorySpec) error {
	if err := dir.RegisterMetric(
		"endpoint",
		&influx.HostAndPort,
		units.None,
		"endpoint of influx server"); err != nil {
		return err
	}
	if err := dir.RegisterMetric(
		"database",
		&influx.Database,
		units.None,
		"database in influx server"); err != nil {
		return err
	}
	if err := dir.RegisterMetric(
		"retentionPolicy",
		&influx.Duration,
		units.None,
		"retention policy of influx server"); err != nil {
		return err
	}
	return nil
}

func registerInfluxes(
	influxes []config.Influx, dir *tricorder.DirectorySpec) error {
	influxesDir, err := dir.RegisterDirectory("influxes")
	if err != nil {
		return err
	}
	for i := range influxes {
		influxDir, err := influxesDir.RegisterDirectory(strconv.Itoa(i))
		if err != nil {
			return err
		}
		if err := registerInflux(influxes[i], influxDir); err != nil {
			return err
		}
	}
	return nil
}

func registerScotty(
	scotty config.Scotty, dir *tricorder.DirectorySpec) error {
	if err := dir.RegisterMetric(
		"endpoint",
		&scotty.HostAndPort,
		units.None,
		"endpoint of scotty server"); err != nil {
		return err
	}
	return nil
}

func registerScotties(
	scotties []config.Scotty, dir *tricorder.DirectorySpec) error {
	scottiesDir, err := dir.RegisterDirectory("scotties")
	if err != nil {
		return err
	}
	for i := range scotties {
		scottyDir, err := scottiesDir.RegisterDirectory(strconv.Itoa(i))
		if err != nil {
			return err
		}
		if err := registerScotty(scotties[i], scottyDir); err != nil {
			return err
		}
	}
	return nil
}

func registerDatabase(
	db config.Database, dir *tricorder.DirectorySpec) error {
	databaseDir, err := dir.RegisterDirectory(db.Name)
	if err != nil {
		return err
	}
	if err := registerInfluxes(db.Influxes, databaseDir); err != nil {
		return err
	}
	if err := registerScotties(db.Scotties, databaseDir); err != nil {
		return err
	}
	return nil
}

func registerProxima(proximaConfig config.Proxima) error {
	tricorder.UnregisterPath(kDatabasesTricorderPath)
	databasesDir, err := tricorder.RegisterDirectory(kDatabasesTricorderPath)
	if err != nil {
		return err
	}
	for _, db := range proximaConfig.Dbs {
		if err := registerDatabase(db, databasesDir); err != nil {
			return err
		}
	}
	return nil
}

func (e *executerType) Names() []string {
	id, p := e.proxima.Get()
	defer e.proxima.Put(id)
	return p.Names()
}

// Query runs a query against multiple influx db instances merging the results
// Query uses the logger instance to report any influx instances that are
// down.
func (e *executerType) Query(
	logger *log.Logger, queryStr, database, epoch string) (
	*client.Response, error) {
	id, p := e.proxima.Get()
	defer e.proxima.Put(id)
	now := time.Now()
	query, err := qlutils.NewQuery(queryStr, now)
	if err != nil {
		return nil, err
	}
	db := p.ByName(database)
	if db == nil {
		return nil, kErrNoSuchDatabase
	}
	return db.Query(logger, query, epoch, now)
}
