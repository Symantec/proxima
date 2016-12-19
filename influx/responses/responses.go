package responses

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"reflect"
	"sort"
)

var (
	errPartialNotSupported = errors.New("Partial rows not supported")
)

func mergeResponses(
	responses []*client.Response) (*client.Response, error) {
	if len(responses) == 0 {
		return &client.Response{}, nil
	}
	// Check for any errors
	for _, response := range responses {
		if response.Error() != nil {
			return &client.Response{Err: response.Error().Error()}, nil
		}
	}

	// Insist that all responses have the same number of results.
	resultLen := len(responses[0].Results)
	for _, response := range responses[1:] {
		if len(response.Results) != resultLen {
			return nil, errors.New(
				"Responses should have equal number of results")
		}
	}

	// Merge the results piecewise
	mergedResultList := make([]client.Result, resultLen)
	for i := range mergedResultList {
		resultsToMerge := make([]client.Result, len(responses))
		for j := range resultsToMerge {
			resultsToMerge[j] = responses[j].Results[i]
		}
		var err error
		mergedResultList[i], err = mergeResults(resultsToMerge)
		if err != nil {
			return nil, err
		}
	}
	return &client.Response{Results: mergedResultList}, nil
}

func mergeResults(results []client.Result) (merged client.Result, err error) {
	var mergedMessages []*client.Message
	var rowPtrs rowPtrListType
	for _, result := range results {
		mergedMessages = append(mergedMessages, result.Messages...)
		rowPtrs = append(rowPtrs, toRowPtrList(result.Series)...)
	}
	sort.Sort(rowPtrs)
	var mergedRows []models.Row

	// Next we have to reduce multiple row instances for the same time series
	// down into one.
	mergedRows, err = reduceRows(rowPtrs)
	if err != nil {
		return
	}
	return client.Result{Series: mergedRows, Messages: mergedMessages}, nil
}

func reduceRows(rows rowPtrListType) (reduced []models.Row, err error) {
	currentRow := rows.Next()
	for currentRow != nil {
		if currentRow.Partial {
			return nil, errPartialNotSupported
		}
		currentRowCopy := *currentRow
		currentRowCopy.Values = nil
		currentRowCopy.Values = append(
			currentRowCopy.Values, currentRow.Values...)
		nextRow := rows.Next()
		for nextRow != nil && compareRows(currentRow, nextRow) == 0 {
			if nextRow.Partial {
				return nil, errPartialNotSupported
			}
			if !reflect.DeepEqual(
				nextRow.Columns, currentRow.Columns) {
				return nil, fmt.Errorf(
					"Columns don't match for Name: %v, Tags: %v",
					currentRow.Name, currentRow.Tags)
			}
			currentRowCopy.Values = append(
				currentRowCopy.Values, nextRow.Values...)
			nextRow = rows.Next()
		}
		if err := sortValuesInPlaceByTime(&currentRowCopy); err != nil {
			return nil, err
		}
		reduced = append(reduced, currentRowCopy)
		currentRow = nextRow
	}
	return
}

func sortValuesInPlaceByTime(row *models.Row) error {
	timeIdx := -1
	for i, columnName := range row.Columns {
		if columnName == "time" {
			timeIdx = i
			break
		}
	}
	if timeIdx == -1 {
		return nil
	}
	if err := sortByTime(row.Values, timeIdx); err != nil {
		return err
	}
	return nil
}

type sortTimeSeriesType struct {
	valuesToSort [][]interface{}
	times        []int64
}

func newSortTimeSeriesType(
	valuesToSort [][]interface{}, timeIdx int) (
	result *sortTimeSeriesType, err error) {
	times := make([]int64, len(valuesToSort))
	for i := range times {
		num, ok := valuesToSort[i][timeIdx].(json.Number)
		if !ok {
			err = fmt.Errorf(
				"Time wrong format %v",
				valuesToSort[i][timeIdx])
			return
		}
		times[i], err = num.Int64()
		if err != nil {
			return
		}
	}
	return &sortTimeSeriesType{valuesToSort: valuesToSort, times: times}, nil
}

func (s *sortTimeSeriesType) Len() int {
	return len(s.valuesToSort)
}

func (s *sortTimeSeriesType) Swap(i, j int) {
	s.valuesToSort[i], s.valuesToSort[j] = s.valuesToSort[j], s.valuesToSort[i]
	s.times[i], s.times[j] = s.times[j], s.times[i]
}

func (s *sortTimeSeriesType) Less(i, j int) bool {
	return s.times[i] < s.times[j]
}

func sortByTime(values [][]interface{}, timeIdx int) error {
	s, err := newSortTimeSeriesType(values, timeIdx)
	if err != nil {
		return err
	}
	sort.Sort(s)
	return nil
}

type rowPtrListType []*models.Row

func toRowPtrList(rows []models.Row) rowPtrListType {
	result := make(rowPtrListType, len(rows))
	for i := range rows {
		result[i] = &rows[i]
	}
	return result
}

func (l rowPtrListType) Len() int { return len(l) }

func (l rowPtrListType) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l rowPtrListType) Less(i, j int) bool {
	return compareRows(l[i], l[j]) < 0
}

func (l *rowPtrListType) Next() *models.Row {
	if len(*l) == 0 {
		return nil
	}
	result := (*l)[0]
	*l = (*l)[1:]
	return result
}

func compareRows(lhs, rhs *models.Row) int {
	if lhs.Name < rhs.Name {
		return -1
	}
	if lhs.Name > rhs.Name {
		return 1
	}
	return compareTags(lhs.Tags, rhs.Tags)
}

func compareTags(lhs, rhs map[string]string) int {
	lkeys := tagKeys(lhs)
	rkeys := tagKeys(rhs)
	return compareTagsByKeys(lkeys, rkeys, lhs, rhs)
}

func compareTagsByKeys(lkeys, rkeys []string, lhs, rhs map[string]string) int {
	llength := len(lkeys)
	rlength := len(rkeys)
	for i := 0; i < llength && i < rlength; i++ {
		if lkeys[i] < rkeys[i] {
			return -1
		}
		if lkeys[i] > rkeys[i] {
			return 1
		}
		lval, rval := lhs[lkeys[i]], rhs[rkeys[i]]
		if lval < rval {
			return -1
		}
		if lval > rval {
			return 1
		}
	}
	if llength < rlength {
		return -1
	}
	if llength > rlength {
		return 1
	}
	return 0
}

func tagKeys(m map[string]string) (result []string) {
	for k := range m {
		result = append(result, k)
	}
	sort.Strings(result)
	return
}
