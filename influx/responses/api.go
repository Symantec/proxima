package responses

import (
	"github.com/influxdata/influxdb/client/v2"
)

// Merge merges responses from multiple servers into a single
// response.
// Merge expects the same query to be sent to all servers except
// for different time ranges.
// An error in any respone means an error in the merged response.
//
// The returned response will contain time series with values sorted by time
// in increasing order even if the responses merged had times in
// decreasing order.
//
// If the returned responses containing multiple series, they will be sorted
// first by name and then by the tags. When sorting tags, Merge
// first places the tag keys of the time series to be sorted in ascending
// order. To compare two sets of tags, Merge first compares the
// first pair of tag keys. If they match, Merge uses the values of
// those first keys to break the tie. If those match, Merge uses
// the second pair of tag keys to break the tie. If those match,
// Merge uses the values of the second keys to brak the tie etc.
//
// Merge includes all messages from the responses being merged in
// the merged response.
func Merge(responses ...*client.Response) (*client.Response, error) {
	return mergeResponses(responses)
}
