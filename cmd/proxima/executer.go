package main

import (
	"errors"
	"fmt"
	"github.com/Symantec/proxima/config"
	"github.com/Symantec/scotty/influx/qlutils"
	"github.com/Symantec/scotty/influx/responses"
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	errNoBackends = errors.New("No backends available to serve query. Check the configuration file.")
)

var (
	kInfluxTricorderPath = "/proc/influx"
)

// Single influx instance
type instance struct {
	Cl client.Client
	// The duration of this instane
	Duration time.Duration
}

// Immutable list of instances sorted from oldest to youngest
type instanceList []instance

func (l instanceList) Len() int {
	return len(l)
}

func (l instanceList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l instanceList) Less(i, j int) bool {
	// Bigger duration means going further back in time
	return l[i].Duration > l[j].Duration
}

// return the min time of instance at given index
func (l instanceList) minTime(i int, now time.Time) time.Time {
	return now.Add(-l[i].Duration)
}

// SplitQuery splits given query across all the instances by time.
// If no error, element i of splitQueries corresponds to element i of this
// slice.
// However, elements in splitQueries will be nil if the query does not apply
// to the corresponding instance in this slice.
//
// If no error, hasUnknownRetentionPolicy is true if one of of the instances
// has unknown retention policy. This instance will always be the very last
// instance in this slice. For this instance, the returned split query is
// always a copy of query itself.
func (l instanceList) SplitQuery(
	query *influxql.Query, now time.Time) (
	splitQueries []*influxql.Query,
	hasUnknownRetentionPolicy bool,
	err error) {
	if len(l) == 0 {
		return
	}
	result := make([]*influxql.Query, len(l))
	var unknownRetentionPolicy bool
	for i := range result {
		min := l.minTime(i, now)
		if min == now {
			// Our source has unknown retention policy. Send defensive copy
			// of entire query.
			queryCopy := *query
			result[i] = &queryCopy
			unknownRetentionPolicy = true
		} else {
			// Query up to the present for each backend. This way if
			// an influx instance with finer grained data goes down,
			// proxima can use an influx instance with courser grained
			// data to fill in the missing times.
			result[i], err = qlutils.QuerySetTimeRange(query, min, now)
			if err != nil {
				return
			}
		}
	}
	return result, unknownRetentionPolicy, nil
}

// executerType executes queries across multiple influx db instances.
// executerType instances are safe to use with multiple goroutines
type executerType struct {
	lock      sync.Mutex
	instances instanceList
}

// newExecuter returns a new instance with no configuration. Querying it
// will always yield errNoBackends.
func newExecuter() *executerType {
	return &executerType{}
}

// SetupWithStream sets up this instance with config file contents in r.
func (e *executerType) SetupWithStream(r io.Reader) error {
	var cluster config.Cluster
	if err := yamlutil.Read(r, &cluster); err != nil {
		return err
	}
	// We allow only one source with missing / zero duration. This is the
	// source for which we do not know the retention policy.
	var zeroDurationFound bool
	// The host and port for each source must be unique
	hostAndPortsSoFar := make(map[string]bool)
	for _, instance := range cluster.Instances {
		if instance.Duration == 0 {
			if zeroDurationFound {
				return errors.New("Only one source with unknown duration allowed.")
			}
			zeroDurationFound = true
		}
		if hostAndPortsSoFar[instance.HostAndPort] {
			return fmt.Errorf("%s listed twice", instance.HostAndPort)
		}
		hostAndPortsSoFar[instance.HostAndPort] = true
	}

	newInstances := make(instanceList, len(cluster.Instances))
	for i := range newInstances {
		cl, err := client.NewHTTPClient(client.HTTPConfig{
			Addr: cluster.Instances[i].HostAndPort,
		})
		if err != nil {
			return err
		}
		newInstances[i] = instance{
			Cl:       cl,
			Duration: cluster.Instances[i].Duration,
		}
	}
	if err := registerMetrics(&cluster); err != nil {
		return err
	}
	sort.Sort(newInstances)
	e.set(newInstances)
	return nil
}

func registerMetrics(cluster *config.Cluster) error {
	tricorder.UnregisterPath(kInfluxTricorderPath)
	influxDir, err := tricorder.RegisterDirectory(kInfluxTricorderPath)
	if err != nil {
		return err
	}
	for _, instance := range cluster.Instances {
		// We need a new "instance" variable with each iteration for holding
		// metric values for tricorder to prevent each iteration from
		// clobbering metric values from the previous iterations.
		instance := instance
		// Replace slashes in URLs with underscores since slashes are path
		// dilimeters.
		instanceDir, err := influxDir.RegisterDirectory(
			strings.Replace(instance.HostAndPort, "/", "_", -1))
		if err != nil {
			return err
		}
		if err := instanceDir.RegisterMetric(
			"duration",
			&instance.Duration,
			units.Second,
			"How far back this instance goes"); err != nil {
			return err
		}
	}
	return nil
}

// We have to compare the error strings because the RPC call to scotty
// prevents the error from scotty from being compared directly.
func isUnsupportedError(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == qlutils.ErrUnsupported.Error()
}

// Query runs a query against multiple influx db instances merging the results
// Query uses the logger instance to report any influx instances that are
// down.
func (e *executerType) Query(
	logger *log.Logger, queryStr, database, epoch string) (
	*client.Response, error) {
	now := time.Now()
	query, err := qlutils.NewQuery(queryStr, now)
	if err == qlutils.ErrNonSelectStatement {
		fetchedInstances := e.get()
		if len(fetchedInstances) == 0 {
			return nil, errNoBackends
		}
		// Just send to the influx with biggest duration that is currently up.
		aQuery := client.NewQuery(queryStr, database, epoch)
		var err error
		for _, instance := range fetchedInstances {
			var result *client.Response
			result, err = instance.Cl.Query(aQuery)
			if err == nil {
				return result, nil
			}
		}
		// If nothing is up, return last error encountered
		return nil, err
	}
	// Oops, error parsing query
	if err != nil {
		return nil, err
	}
	fetchedInstances := e.get()
	querySplits, unknownRetentionPolicyPresent, err := fetchedInstances.SplitQuery(query, now)
	// Oops, error splitting query
	if err != nil {
		return nil, err
	}

	// Got to have at least one split
	if len(querySplits) == 0 {
		return nil, errNoBackends
	}

	// These are placeholders for the response and error from each influx db
	// instance. These slices may be shorter than the original querySplits
	// slice since querySplits may have nil elements indicating that
	// corresponding backend need not be queried.
	// responseIdx = effective length of these slices.
	responseList := make([]*client.Response, len(querySplits))
	errs := make([]error, len(querySplits))

	var wg sync.WaitGroup
	responseIdx := 0
	for instanceIdx, querySplit := range querySplits {
		// Query not applicable to this instance, skip
		if querySplit == nil {
			continue
		}
		wg.Add(1)
		go func(
			cl client.Client,
			query string,
			responseHere **client.Response,
			errHere *error) {
			*responseHere, *errHere = cl.Query(
				client.NewQuery(query, database, epoch))
			wg.Done()
		}(fetchedInstances[instanceIdx].Cl,
			querySplit.String(),
			&responseList[responseIdx],
			&errs[responseIdx])
		responseIdx++
	}
	wg.Wait()

	if unknownRetentionPolicyPresent {
		// Remove special response from repsonseList slice.
		responseIdx--
	}

	// These will be the responses from influx servers that we merge
	// This is a subset of the responses in responseList since some of
	// those influx servers may be unreachable.
	var responsesToMerge []*client.Response

	// In case none of the responses are viable, report this error
	// back to client
	var lastErrorEncountered error

	// Build responsesToMerge
	for i, err := range errs[:responseIdx] {
		// If we could reach the source, use its response
		if err == nil {
			responsesToMerge = append(responsesToMerge, responseList[i])
		} else {
			logger.Println(err)
			lastErrorEncountered = err
		}
	}
	// This is the response from the special source with unknown retention
	// policy. It gets merged separately at the end. May remain nil.
	var specialResponse *client.Response

	if unknownRetentionPolicyPresent {
		// The response from the source with unknown retention
		// policy is always in this location.
		if errs[responseIdx] == nil {
			// The only time we don't use the special source is if
			// there are backend influx servers AND the special source
			// doesn't support the query.
			if len(responsesToMerge) == 0 || !isUnsupportedError(responseList[responseIdx].Error()) {
				specialResponse = responseList[responseIdx]
			}
		} else {
			logger.Println(errs[responseIdx])
			lastErrorEncountered = errs[responseIdx]
		}
	}
	// If we reached ordinary influx servers with known retention policy,
	// merge those results and then merge the response from the special
	// source with unknown retention policy last.
	if len(responsesToMerge) != 0 {
		mergedResponse, err := responses.Merge(responsesToMerge...)
		if err != nil {
			return nil, err
		}
		if specialResponse != nil {
			return responses.MergePreferred(mergedResponse, specialResponse)
		}
		return mergedResponse, nil
	}
	// We only have the source with unknown retention policy, return as-is
	if specialResponse != nil {
		return specialResponse, nil
	}
	return nil, lastErrorEncountered
}

func (e *executerType) set(instances instanceList) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.instances = instances
}

func (e *executerType) get() instanceList {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.instances
}
