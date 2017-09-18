// Package config contains the datastructures representing the configuration
// of proxima.
package config

import (
	"github.com/Symantec/scotty/lib/yamlutil"
	"time"
)

// Influx represents a single influx backend.
type Influx struct {
	// http://someHost.com:1234.
	HostAndPort string `yaml:"hostAndPort"`
	// The duration in this instance's retention policy
	Duration time.Duration `yaml:"duration"`
	// The influx Database to use
	Database string `yaml:"database"`
}

func (i *Influx) UnmarshalYAML(
	unmarshal func(interface{}) error) error {
	type influxFields Influx
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*influxFields)(i))
}

// InfluxList represents a group of influx backends.
// InfluxList instances are to be treated as immutable.
type InfluxList []Influx

// Order returns an InfluxList like this one but ordered by duration
// in descending order
func (i InfluxList) Order() InfluxList {
	result := make(InfluxList, len(i))
	copy(result, i)
	orderInfluxes(result)
	return result
}

// Scotty represents a single scotty server, a list of redundant scotty
// servers each having the same data, or a list of scotty servers where
// each scotty server has different data. One and only one of the fields
// must be filled in.
type Scotty struct {
	// http://someHost.com:1234.
	HostAndPort string `yaml:"hostAndPort"`
	// Scotty servers have the same data
	Scotties ScottyList `yaml:"scotties"`
	// Scotty servers have different data
	Partials ScottyList `yaml:"partials"`
}

func (s *Scotty) UnmarshalYAML(
	unmarshal func(interface{}) error) error {
	type scottyFields Scotty
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*scottyFields)(s))
}

// ScottyList represents a group of scotty servers.
// ScottyList instances are to be treated as immutable.
type ScottyList []Scotty

// Database represents a single named configuration within proxima.
type Database struct {
	// Name of configuration
	Name string `yaml:"name"`
	// The influx backends
	Influxes InfluxList `yaml:"influxes"`
	// The scotty servers
	Scotties ScottyList `yaml:"scotties"`
}

func (d *Database) UnmarshalYAML(
	unmarshal func(interface{}) error) error {
	type databaseFields Database
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*databaseFields)(d))
}

// Proxima represents the configuration of proxima. This is what is read
// from the configuration file using the yamlutil package.
type Proxima struct {
	Dbs []Database `yaml:"databases"`
}

func (p *Proxima) Reset() {
	*p = Proxima{}
}

func (p *Proxima) UnmarshalYAML(
	unmarshal func(interface{}) error) error {
	type proximaFields Proxima
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*proximaFields)(p))
}
