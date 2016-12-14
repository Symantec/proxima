package responses_test

import (
	"encoding/json"
	"github.com/Symantec/proxima/influx/responses"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	. "github.com/smartystreets/goconvey/convey"
	"strconv"
	"testing"
)

func newPoint(x int64) []interface{} {
	return []interface{}{nil, json.Number(strconv.FormatInt(x, 10)), nil}
}

func TestMergeMessages(t *testing.T) {
	Convey("Merging no responses yields zero reponse", t, func() {
		resp, err := responses.Merge()
		So(*resp, ShouldBeZeroValue)
		So(err, ShouldBeNil)
	})

	Convey("Given responses with different messages", t, func() {
		message1 := &client.Message{Level: "1", Text: "one"}
		message2 := &client.Message{Level: "2", Text: "two"}
		message3 := &client.Message{Level: "3", Text: "three"}
		message4 := &client.Message{Level: "4", Text: "four"}
		message5 := &client.Message{Level: "5", Text: "five"}

		message11 := &client.Message{Level: "11", Text: "eleven"}
		message12 := &client.Message{Level: "12", Text: "twelve"}
		message13 := &client.Message{Level: "13", Text: "thirteen"}

		response1 := &client.Response{
			Results: []client.Result{
				{
					Messages: []*client.Message{
						message1,
						message2,
					},
				},
				{
					Messages: []*client.Message{
						message11,
						message12,
					},
				},
			},
		}
		response2 := &client.Response{
			Results: []client.Result{
				{
					Messages: []*client.Message{
						message3,
						message4,
						message5,
					},
				},
				{
					Messages: []*client.Message{
						message13,
					},
				},
			},
		}
		mergedResponses, err := responses.Merge(
			response1, response2)
		So(err, ShouldBeNil)
		Convey("Messages should be merged", func() {
			expected := &client.Response{
				Results: []client.Result{
					{
						Messages: []*client.Message{
							message1,
							message2,
							message3,
							message4,
							message5,
						},
					},
					{
						Messages: []*client.Message{
							message11,
							message12,
							message13,
						},
					},
				},
			}
			So(mergedResponses, ShouldResemble, expected)
		})
	})
}

func TestMerge(t *testing.T) {

	Convey("Given 3 responses with 2 results each", t, func() {
		valueTimeValueColumns := []string{"value1", "time", "value2"}

		apoint10 := newPoint(1010)
		apoint20 := newPoint(1020)
		apoint30 := newPoint(1030)

		bpoint5 := newPoint(2005)
		bpoint10 := newPoint(2010)
		bpoint15 := newPoint(2015)
		bpoint20 := newPoint(2020)
		bpoint25 := newPoint(2025)
		bpoint30 := newPoint(2030)

		cpoint10 := newPoint(3010)

		dpoint5 := newPoint(4005)
		dpoint10 := newPoint(4010)
		dpoint15 := newPoint(4015)
		dpoint20 := newPoint(4020)
		dpoint25 := newPoint(4025)
		dpoint27 := newPoint(4027)
		dpoint30 := newPoint(4030)
		dpoint37 := newPoint(4037)
		dpoint47 := newPoint(4047)

		epoint10 := newPoint(5010)
		epoint20 := newPoint(5020)

		a1Values := [][]interface{}{apoint10, apoint20, apoint30}
		b1Values := [][]interface{}{bpoint10, bpoint20, bpoint30}
		c1Values := [][]interface{}{cpoint10}
		d1Values := [][]interface{}{dpoint5, dpoint15, dpoint25}
		b2Values := [][]interface{}{bpoint5, bpoint15, bpoint25}
		d2Values := [][]interface{}{dpoint10, dpoint20, dpoint30}
		e2Values := [][]interface{}{epoint10, epoint20}
		d3Values := [][]interface{}{dpoint27, dpoint37, dpoint47}
		result1_1 := client.Result{
			Series: []models.Row{
				{
					Name:    "series a",
					Columns: valueTimeValueColumns,
					Values:  a1Values,
				},
				{
					Name:    "series b",
					Columns: valueTimeValueColumns,
					Values:  b1Values,
				},
				{
					Name:    "series c",
					Columns: valueTimeValueColumns,
					Values:  c1Values,
				},
				{
					Name:    "series d",
					Columns: valueTimeValueColumns,
					Values:  d1Values,
				},
			},
		}
		result1_2 := client.Result{
			Series: []models.Row{
				{
					Name:    "series b",
					Columns: valueTimeValueColumns,
					Values:  b2Values,
				},
				{
					Name:    "series d",
					Columns: valueTimeValueColumns,
					Values:  d2Values,
				},
				{
					Name:    "series e",
					Columns: valueTimeValueColumns,
					Values:  e2Values,
				},
			},
		}
		result1_3 := client.Result{
			Series: []models.Row{
				{
					Name:    "series d",
					Columns: valueTimeValueColumns,
					Values:  d3Values,
				},
			},
		}
		result2_1 := client.Result{
			Series: []models.Row{
				{
					Name: "foxtrot",
					Tags: map[string]string{
						"Bear":   "Yogi",
						"Forest": "Yosemeti",
					},
				},
				{
					Name: "foxtrot",
					Tags: map[string]string{
						"Bear":   "Yogi",
						"Forest": "Jellystone",
					},
				},
			},
		}
		result2_2 := client.Result{
			Series: []models.Row{
				{
					Name: "bravo",
					Tags: map[string]string{
						"Bear":   "Yogi",
						"Forest": "Yosemeti",
					},
				},
				{
					Name: "bravo",
					Tags: map[string]string{
						"Bear":    "Yogi",
						"Country": "Zimbabwe",
					},
				},
			},
		}
		result2_3 := client.Result{
			Series: []models.Row{
				{
					Name: "foxtrot",
					Tags: map[string]string{
						"Bear":   "Yogi",
						"Forest": "Jellystone",
						"Go":     "go",
					},
				},
				{
					Name: "foxtrot",
					Tags: map[string]string{
						"Bear": "Yogi",
					},
				},
			},
		}
		response1 := &client.Response{Results: []client.Result{result1_1, result2_1}}
		response2 := &client.Response{Results: []client.Result{result1_2, result2_2}}
		response3 := &client.Response{Results: []client.Result{result1_3, result2_3}}

		mergedResponse, err := responses.Merge(
			response1, response2, response3)
		So(err, ShouldBeNil)

		Convey("The first result from each of the three responses containing time series: A1, B1, C1, D1; B2, D2, E2; and D3 should yield merged result with time series A1, B12, C1, D123, E2.", func() {

			b12Values := [][]interface{}{
				bpoint5, bpoint10, bpoint15, bpoint20, bpoint25, bpoint30}
			d123Values := [][]interface{}{
				dpoint5,
				dpoint10,
				dpoint15,
				dpoint20,
				dpoint25,
				dpoint27,
				dpoint30,
				dpoint37,
				dpoint47}

			expectedResult := client.Result{
				Series: []models.Row{
					{
						Name:    "series a",
						Columns: valueTimeValueColumns,
						Values:  a1Values,
					},
					{
						Name:    "series b",
						Columns: valueTimeValueColumns,
						Values:  b12Values,
					},
					{
						Name:    "series c",
						Columns: valueTimeValueColumns,
						Values:  c1Values,
					},
					{
						Name:    "series d",
						Columns: valueTimeValueColumns,
						Values:  d123Values,
					},
					{
						Name:    "series e",
						Columns: valueTimeValueColumns,
						Values:  e2Values,
					},
				},
			}
			So(mergedResponse.Results[0], ShouldResemble, expectedResult)
		})

		Convey("The second result from each containing time series sorted by name first then by tags", func() {

			expectedResult := client.Result{
				Series: []models.Row{
					{
						Name: "bravo",
						Tags: map[string]string{
							"Bear":    "Yogi",
							"Country": "Zimbabwe",
						},
					},
					{
						Name: "bravo",
						Tags: map[string]string{
							"Bear":   "Yogi",
							"Forest": "Yosemeti",
						},
					},
					{
						Name: "foxtrot",
						Tags: map[string]string{
							"Bear": "Yogi",
						},
					},
					{
						Name: "foxtrot",
						Tags: map[string]string{
							"Bear":   "Yogi",
							"Forest": "Jellystone",
						},
					},
					{
						Name: "foxtrot",
						Tags: map[string]string{
							"Bear":   "Yogi",
							"Forest": "Jellystone",
							"Go":     "go",
						},
					},
					{
						Name: "foxtrot",
						Tags: map[string]string{
							"Bear":   "Yogi",
							"Forest": "Yosemeti",
						},
					},
				},
			}
			So(mergedResponse.Results[1], ShouldResemble, expectedResult)
		})

		Convey("Given at least one response with an error", func() {

			// Introduce a random error
			response2.Results[1].Err = "An error"
			mergedResponse, err := responses.Merge(
				response1, response2, response3)
			So(err, ShouldBeNil)
			Convey("Merged Response will have an error", func() {
				So(mergedResponse.Error(), ShouldNotBeNil)
			})
		})

		Convey("Given at least one response has partial = true", func() {

			// Introduce a random Partial = true
			response1.Results[0].Series[2].Partial = true
			_, err := responses.Merge(response1, response2, response3)

			Convey("Merge returns error", func() {
				So(err, ShouldNotBeNil)
			})

		})

		Convey("Given that each response has different result count", func() {

			response1.Results = append(response1.Results, client.Result{})
			_, err := responses.Merge(response1, response2, response3)

			Convey("Merge returns error", func() {
				So(err, ShouldNotBeNil)
			})

		})

		Convey("Given mismatched columns for same time series", func() {

			// Change series B in first response as it gets merged
			response1.Results[0].Series[1].Columns = []string{"a", "b", "c"}
			_, err := responses.Merge(response1, response2, response3)

			Convey("Merge returns error", func() {
				So(err, ShouldNotBeNil)
			})
		})

	})

}
