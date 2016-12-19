package config

import (
	"github.com/Symantec/scotty/lib/yamlutil"
	"time"
)

// Instance represents a single influx instance
type Instance struct {
	// http://someHost.com:1234.
	HostAndPort string `yaml:"hostAndPort"`
	// The duration in this instance's retention policy
	Duration time.Duration `yaml:"duration"`
}

func (i *Instance) Reset() {
	*i = Instance{}
}

func (i *Instance) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type instanceFields Instance
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*instanceFields)(i))
}

// Cluster represents a group of influx instances
type Cluster struct {
	// The instances
	Instances []Instance `yaml:"instances"`
}

func (c *Cluster) Reset() {
	*c = Cluster{}
}

func (c *Cluster) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type clusterFields Cluster
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*clusterFields)(c))
}
