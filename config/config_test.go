package config_test

import (
	"bytes"
	"github.com/Symantec/proxima/config"
	"github.com/Symantec/scotty/lib/yamlutil"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestConfig(t *testing.T) {

	Convey("Normal config", t, func() {
		configContents := `
databases:
- name: foo
  influxes:
  - hostAndPort: influx1
    duration: 1h
    database: tenant
  - hostAndPort: influx2
    duration: 100h
    database: mongo
  scotties:
  - hostAndPort: scotty1
  - hostAndPort: scotty2
- name: bar
  scotties:
  - hostAndPort: scotty11
  - hostAndPort: scotty12
  - partials:
    - hostAndPort: scotty21
    - hostAndPort: scotty22
  - scotties:
    - hostAndPort: scotty31
    - hostAndPort: scotty32
`
		buffer := bytes.NewBuffer(([]byte)(configContents))
		var proxima config.Proxima
		So(yamlutil.Read(buffer, &proxima), ShouldBeNil)
		So(proxima, ShouldResemble, config.Proxima{
			Dbs: []config.Database{
				{
					Name: "foo",
					Influxes: config.InfluxList{
						{
							HostAndPort: "influx1",
							Duration:    time.Hour,
							Database:    "tenant",
						},
						{
							HostAndPort: "influx2",
							Duration:    100 * time.Hour,
							Database:    "mongo",
						},
					},
					Scotties: config.ScottyList{
						{HostAndPort: "scotty1"},
						{HostAndPort: "scotty2"},
					},
				},
				{
					Name: "bar",
					Scotties: config.ScottyList{
						{HostAndPort: "scotty11"},
						{HostAndPort: "scotty12"},
						{Partials: config.ScottyList{
							{HostAndPort: "scotty21"},
							{HostAndPort: "scotty22"},
						}},
						{Scotties: config.ScottyList{
							{HostAndPort: "scotty31"},
							{HostAndPort: "scotty32"},
						}},
					},
				},
			},
		})
	})

	Convey("config with bad name", t, func() {
		configContents := `
databases:
- name: foo
  influxes:
  - hostAndPort: influx1
    duration: 1h
    database: tenant
  - hostAndPort: influx2
    badfield: 100h
    database: mongo
  scotties:
  - hostAndPort: scotty1
  - hostAndPort: scotty2
- name: bar
  scotties:
  - hostAndPort: scotty11
  - hostAndPort: scotty12
`
		buffer := bytes.NewBuffer(([]byte)(configContents))
		var proxima config.Proxima
		So(yamlutil.Read(buffer, &proxima), ShouldNotBeNil)
	})

	Convey("Sort by duration", t, func() {
		orig := config.InfluxList{
			config.Influx{Database: "mo", Duration: 5 * time.Hour},
			config.Influx{Database: "elf", Duration: 100 * time.Hour},
			config.Influx{Database: "sam", Duration: 5 * time.Hour},
			config.Influx{Database: "a", Duration: 100 * time.Hour},
		}
		ordered := orig.Order()
		So(ordered, ShouldResemble, config.InfluxList{
			config.Influx{Database: "elf", Duration: 100 * time.Hour},
			config.Influx{Database: "a", Duration: 100 * time.Hour},
			config.Influx{Database: "mo", Duration: 5 * time.Hour},
			config.Influx{Database: "sam", Duration: 5 * time.Hour},
		})
		// quick check that orig didn't change
		So(orig[0].Database, ShouldEqual, "mo")
	})
}
