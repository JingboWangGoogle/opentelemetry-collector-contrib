// Copyright 2020 OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metricstransformprocessor

import (
	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	"github.com/golang/protobuf/ptypes/timestamp"
	"go.opentelemetry.io/collector/consumer/consumerdata"
)

const (
	metric1         = "metric1"
	metric2         = "metric2"
	label1          = "label1"
	label2          = "label2"
	labelValue11    = "label1-value1"
	labelValue21    = "label2-value1"
	labelValue12    = "label1-value2"
	labelValue22    = "label2-value2"
	newMetric1      = "metric1/new"
	newMetric2      = "metric2/new"
	newLabel1       = "label1/new"
	newLabelValue11 = "label1-value1/new"
	nonexist        = "nonexist"
)

type testPoint struct {
	timestamp             int64
	value                 int
	count                 int64
	sum                   float64
	bounds                []float64
	buckets               []int64
	sumOfSquaredDeviation float64
	isInt64               bool
	isDouble              bool
}

type testTimeseries struct {
	startTimestamp int64
	labelValues    []string
	points         []testPoint
}

type testMetric struct {
	name       string
	labelKeys  []string
	timeseries []testTimeseries
}

type metricsTransformTest struct {
	name       string // test name
	transforms []Transform
	in         []testMetric
	out        []testMetric
}

var (
	// test metrics
	initialMetric = testMetric{
		name:      metric1,
		labelKeys: []string{label1, label2},
		timeseries: []testTimeseries{
			{
				labelValues: []string{labelValue11, labelValue21},
			},
		},
	}

	initialMetricRename1 = testMetric{
		name: metric1,
	}

	outMetricRename1 = testMetric{
		name: newMetric1,
	}

	initialMetricRename2 = testMetric{
		name: metric2,
	}

	outMetricRename2 = testMetric{
		name: newMetric2,
	}

	initialMetricLabelRename1 = testMetric{
		name:      metric1,
		labelKeys: []string{label1, label2},
	}

	outMetricLabelRenameUpdate1 = testMetric{
		name:      metric1,
		labelKeys: []string{newLabel1, label2},
	}

	outMetricLabelRenameInsert1 = testMetric{
		name:      newMetric1,
		labelKeys: []string{newLabel1, label2},
	}

	initialLabelValueRename1 = testMetric{
		name:      metric1,
		labelKeys: []string{label1},
		timeseries: []testTimeseries{
			{
				labelValues: []string{labelValue11},
			},
			{
				labelValues: []string{labelValue12},
			},
		},
	}

	outLabelValueRenameUpdate1 = testMetric{
		name:      metric1,
		labelKeys: []string{label1},
		timeseries: []testTimeseries{
			{
				labelValues: []string{newLabelValue11},
			},
			{
				labelValues: []string{labelValue12},
			},
		},
	}

	outLabelValueRenameInsert1 = testMetric{
		name:      newMetric1,
		labelKeys: []string{label1},
		timeseries: []testTimeseries{
			{
				labelValues: []string{newLabelValue11},
			},
			{
				labelValues: []string{labelValue12},
			},
		},
	}

	// operations
	validUpateLabelOperation = Operation{
		Action:   UpdateLabel,
		Label:    label1,
		NewLabel: newLabel1,
	}

	invalidUpdateLabelOperation = Operation{
		Action:   UpdateLabel,
		Label:    label1,
		NewLabel: label2,
	}

	validUpdateLabelValueOperation = Operation{
		Action: UpdateLabel,
		Label:  label1,
		ValueActions: []ValueAction{
			{
				Value:    labelValue11,
				NewValue: newLabelValue11,
			},
		},
	}

	invalidUpdateLabelValueOperation = Operation{
		Action: UpdateLabel,
		Label:  label1,
		ValueActions: []ValueAction{
			{
				Value:    labelValue11,
				NewValue: labelValue12,
			},
		},
	}

	validUpdateLabelAggrSumOperation = Operation{
		Action:          AggregateLabels,
		LabelSet:        []string{label1},
		AggregationType: Sum,
	}

	validUpdateLabelAggrAverageOperation = Operation{
		Action:          AggregateLabels,
		LabelSet:        []string{label1},
		AggregationType: Average,
	}

	validUpdateLabelAggrMaxOperation = Operation{
		Action:          AggregateLabels,
		LabelSet:        []string{label1},
		AggregationType: Max,
	}

	validUpdateLabelValuesAggrSumOperation = Operation{
		Action:           AggregateLabelValues,
		Label:            label1,
		AggregatedValues: []string{labelValue11, labelValue12},
		NewValue:         newLabelValue11,
		AggregationType:  Sum,
	}

	validUpdateLabelValuesAggrAverageOperation = Operation{
		Action:           AggregateLabelValues,
		Label:            label1,
		AggregatedValues: []string{labelValue11, labelValue12},
		NewValue:         newLabelValue11,
		AggregationType:  Average,
	}

	validUpdateLabelValuesAggrMaxOperation = Operation{
		Action:           AggregateLabelValues,
		Label:            label1,
		AggregatedValues: []string{labelValue11, labelValue12},
		NewValue:         newLabelValue11,
		AggregationType:  Max,
	}

	// test cases
	standardTests = []metricsTransformTest{
		// UPDATE
		{
			name: "metric_name_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					NewName:    newMetric1,
				},
			},
			in:  []testMetric{initialMetricRename1},
			out: []testMetric{outMetricRename1},
		},
		{
			name: "metric_name_update_multiple",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					NewName:    newMetric1,
				},
				{
					MetricName: metric2,
					Action:     Update,
					NewName:    newMetric2,
				},
			},
			in:  []testMetric{initialMetricRename1, initialMetricRename2},
			out: []testMetric{outMetricRename1, outMetricRename2},
		},
		{
			name: "metric_name_update_nonexist",
			transforms: []Transform{
				{
					MetricName: nonexist,
					Action:     Update,
					NewName:    newMetric1,
				},
			},
			in:  []testMetric{initialMetricRename1},
			out: []testMetric{initialMetricRename1},
		},
		{
			name: "metric_name_update_invalid",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					NewName:    metric2,
				},
			},
			in:  []testMetric{initialMetricRename1, initialMetricRename2},
			out: []testMetric{initialMetricRename1, initialMetricRename2},
		},
		{
			name: "metric_label_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpateLabelOperation},
				},
			},
			in:  []testMetric{initialMetricLabelRename1},
			out: []testMetric{outMetricLabelRenameUpdate1},
		},
		{
			name: "metric_label_update_invalid",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{invalidUpdateLabelOperation},
				},
			},
			in:  []testMetric{initialMetricLabelRename1},
			out: []testMetric{initialMetricLabelRename1},
		},
		{
			name: "metric_label_value_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpdateLabelValueOperation},
				},
			},
			in:  []testMetric{initialLabelValueRename1},
			out: []testMetric{outLabelValueRenameUpdate1},
		},
		{
			name: "metric_label_value_update_invalid",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{invalidUpdateLabelValueOperation},
				},
			},
			in:  []testMetric{initialLabelValueRename1},
			out: []testMetric{initialLabelValueRename1},
		},
		{
			name: "metric_label_aggregation_sum_int_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpdateLabelAggrSumOperation},
				},
			},
			in:  []testMetric{inLabelAggrBuilder(1, 3, true, false)},
			out: []testMetric{outLabelAggrBuilder(4, true, false)},
		},
		{
			name: "metric_label_aggregation_average_int_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpdateLabelAggrAverageOperation},
				},
			},
			in:  []testMetric{inLabelAggrBuilder(1, 3, true, false)},
			out: []testMetric{outLabelAggrBuilder(2, true, false)},
		},
		{
			name: "metric_label_aggregation_max_int_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpdateLabelAggrMaxOperation},
				},
			},
			in:  []testMetric{inLabelAggrBuilder(3, 1, true, false)},
			out: []testMetric{outLabelAggrBuilder(3, true, false)},
		},
		{
			name: "metric_label_aggregation_sum_double_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpdateLabelAggrSumOperation},
				},
			},
			in:  []testMetric{inLabelAggrBuilder(1, 3, false, true)},
			out: []testMetric{outLabelAggrBuilder(4, false, true)},
		},
		{
			name: "metric_label_aggregation_average_double_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpdateLabelAggrAverageOperation},
				},
			},
			in:  []testMetric{inLabelAggrBuilder(1, 3, false, true)},
			out: []testMetric{outLabelAggrBuilder(2, false, true)},
		},
		{
			name: "metric_label_aggregation_max_double_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpdateLabelAggrMaxOperation},
				},
			},
			in:  []testMetric{inLabelAggrBuilder(3, 1, false, true)},
			out: []testMetric{outLabelAggrBuilder(3, false, true)},
		},
		{
			name: "metric_label_values_aggregation_sum_int_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpdateLabelValuesAggrSumOperation},
				},
			},
			in:  []testMetric{inLabelValuesAggrBuilder(3, 1, true, false)},
			out: []testMetric{outLabelValuesAggrBuilder(4, true, false)},
		},
		{
			name: "metric_label_values_aggregation_average_int_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpdateLabelValuesAggrAverageOperation},
				},
			},
			in:  []testMetric{inLabelValuesAggrBuilder(3, 1, true, false)},
			out: []testMetric{outLabelValuesAggrBuilder(2, true, false)},
		},
		{
			name: "metric_label_values_aggregation_max_int_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpdateLabelValuesAggrMaxOperation},
				},
			},
			in:  []testMetric{inLabelValuesAggrBuilder(3, 1, true, false)},
			out: []testMetric{outLabelValuesAggrBuilder(3, true, false)},
		},
		{
			name: "metric_label_values_aggregation_sum_double_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpdateLabelValuesAggrSumOperation},
				},
			},
			in:  []testMetric{inLabelValuesAggrBuilder(3, 1, false, true)},
			out: []testMetric{outLabelValuesAggrBuilder(4, false, true)},
		},
		{
			name: "metric_label_values_aggregation_average_double_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpdateLabelValuesAggrAverageOperation},
				},
			},
			in:  []testMetric{inLabelValuesAggrBuilder(3, 1, false, true)},
			out: []testMetric{outLabelValuesAggrBuilder(2, false, true)},
		},
		{
			name: "metric_label_values_aggregation_max_double_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpdateLabelValuesAggrMaxOperation},
				},
			},
			in:  []testMetric{inLabelValuesAggrBuilder(3, 1, false, true)},
			out: []testMetric{outLabelValuesAggrBuilder(3, false, true)},
		},
		{
			name: "metric_label_values_aggregation_sum_distribution_update",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Update,
					Operations: []Operation{validUpdateLabelAggrSumOperation},
				},
			},
			in: []testMetric{distValueMetricBuilder(
				[]string{label1, label2},
				[][]string{
					{labelValue11, labelValue21},
					{labelValue11, labelValue22},
				},
				[]int64{1, 3},
				[][]testPoint{
					{
						{
							timestamp:             1,
							count:                 3,
							sum:                   6,
							bounds:                []float64{1, 2},
							buckets:               []int64{0, 1, 2},
							sumOfSquaredDeviation: 3,
						},
					},
					{
						{
							timestamp:             1,
							count:                 5,
							sum:                   10,
							bounds:                []float64{1, 2},
							buckets:               []int64{1, 1, 3},
							sumOfSquaredDeviation: 4,
						},
					},
				},
			),
			},
			out: []testMetric{
				distValueMetricBuilder(
					[]string{label1},
					[][]string{
						{labelValue11},
					},
					[]int64{1},
					[][]testPoint{
						{
							{
								timestamp:             1,
								count:                 8,
								sum:                   16,
								bounds:                []float64{1, 2},
								buckets:               []int64{1, 2, 5},
								sumOfSquaredDeviation: 7,
							},
						},
					},
				),
			},
		},
		// INSERT
		{
			name: "metric_name_insert",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Insert,
					NewName:    newMetric1,
				},
			},
			in:  []testMetric{initialMetricRename1},
			out: []testMetric{initialMetricRename1, outMetricRename1},
		},
		{
			name: "metric_name_insert_multiple",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Insert,
					NewName:    newMetric1,
				},
				{
					MetricName: metric2,
					Action:     Insert,
					NewName:    newMetric2,
				},
			},
			in:  []testMetric{initialMetricRename1, initialMetricRename2},
			out: []testMetric{initialMetricRename1, initialMetricRename2, outMetricRename1, outMetricRename2},
		},
		{
			name: "metric_label_update_with_metric_insert",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Insert,
					NewName:    newMetric1,
					Operations: []Operation{validUpateLabelOperation},
				},
			},
			in:  []testMetric{initialMetricLabelRename1},
			out: []testMetric{initialMetricLabelRename1, outMetricLabelRenameInsert1},
		},
		{
			name: "metric_label_value_update_with_metric_insert",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Insert,
					NewName:    newMetric1,
					Operations: []Operation{validUpdateLabelValueOperation},
				},
			},
			in:  []testMetric{initialLabelValueRename1},
			out: []testMetric{initialLabelValueRename1, outLabelValueRenameInsert1},
		},
		{
			name: "metric_label_aggregation_sum_int_insert",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Insert,
					Operations: []Operation{validUpdateLabelAggrSumOperation},
				},
			},
			in:  []testMetric{inLabelAggrBuilder(1, 3, true, false)},
			out: []testMetric{inLabelAggrBuilder(1, 3, true, false), outLabelAggrBuilder(4, true, false)},
		},
		{
			name: "metric_label_values_aggregation_sum_int_insert",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Insert,
					Operations: []Operation{validUpdateLabelValuesAggrSumOperation},
				},
			},
			in:  []testMetric{inLabelValuesAggrBuilder(1, 3, true, false)},
			out: []testMetric{inLabelValuesAggrBuilder(1, 3, true, false), outLabelValuesAggrBuilder(4, true, false)},
		},
		{
			name: "metric_label_values_aggregation_sum_distribution_insert",
			transforms: []Transform{
				{
					MetricName: metric1,
					Action:     Insert,
					Operations: []Operation{validUpdateLabelAggrSumOperation},
				},
			},
			in: []testMetric{
				distValueMetricBuilder(
					[]string{label1, label2},
					[][]string{
						{labelValue11, labelValue21},
						{labelValue11, labelValue22},
					},
					[]int64{1, 3},
					[][]testPoint{
						{
							{
								timestamp:             1,
								count:                 3,
								sum:                   6,
								bounds:                []float64{1, 2},
								buckets:               []int64{0, 1, 2},
								sumOfSquaredDeviation: 3,
							},
						},
						{
							{
								timestamp:             1,
								count:                 5,
								sum:                   10,
								bounds:                []float64{1, 2},
								buckets:               []int64{1, 1, 3},
								sumOfSquaredDeviation: 4,
							},
						},
					},
				),
			},
			out: []testMetric{
				distValueMetricBuilder(
					[]string{label1, label2},
					[][]string{
						{labelValue11, labelValue21},
						{labelValue11, labelValue22},
					},
					[]int64{1, 3},
					[][]testPoint{
						{
							{
								timestamp:             1,
								count:                 3,
								sum:                   6,
								bounds:                []float64{1, 2},
								buckets:               []int64{0, 1, 2},
								sumOfSquaredDeviation: 3,
							},
						},
						{
							{
								timestamp:             1,
								count:                 5,
								sum:                   10,
								bounds:                []float64{1, 2},
								buckets:               []int64{1, 1, 3},
								sumOfSquaredDeviation: 4,
							},
						},
					},
				),
				distValueMetricBuilder(
					[]string{label1},
					[][]string{
						{labelValue11},
					},
					[]int64{1},
					[][]testPoint{
						{
							{
								timestamp:             1,
								count:                 8,
								sum:                   16,
								bounds:                []float64{1, 2},
								buckets:               []int64{1, 2, 5},
								sumOfSquaredDeviation: 7,
							},
						},
					},
				),
			},
		},
	}
)

// constructTestInputMetricsDate builds the actual metrics from the test cases
func constructTestInputMetricsData(test metricsTransformTest) consumerdata.MetricsData {
	md := consumerdata.MetricsData{
		Metrics: make([]*metricspb.Metric, len(test.in)),
	}
	for idx, in := range test.in {
		// construct label keys
		labels := make([]*metricspb.LabelKey, len(in.labelKeys))
		for lidx, l := range in.labelKeys {
			labels[lidx] = &metricspb.LabelKey{
				Key: l,
			}
		}
		// construct timeseries with label values and points
		timeseries := make([]*metricspb.TimeSeries, len(in.timeseries))
		for tidx, ts := range in.timeseries {
			labelValues := make([]*metricspb.LabelValue, len(ts.labelValues))
			for vidx, value := range ts.labelValues {
				labelValues[vidx] = &metricspb.LabelValue{
					Value: value,
				}
			}
			points := make([]*metricspb.Point, len(ts.points))
			for pidx, p := range ts.points {
				points[pidx] = &metricspb.Point{
					Timestamp: &timestamp.Timestamp{
						Seconds: p.timestamp,
						Nanos:   0,
					},
				}
				if p.isInt64 {
					points[pidx].Value = &metricspb.Point_Int64Value{
						Int64Value: int64(p.value),
					}
				} else if p.isDouble {
					points[pidx].Value = &metricspb.Point_DoubleValue{
						DoubleValue: float64(p.value),
					}
				} else {
					buckets := make([]*metricspb.DistributionValue_Bucket, len(p.buckets))
					for buIdx, bucket := range p.buckets {
						buckets[buIdx] = &metricspb.DistributionValue_Bucket{
							Count: bucket,
						}
					}
					points[pidx].Value = &metricspb.Point_DistributionValue{
						DistributionValue: &metricspb.DistributionValue{
							BucketOptions: &metricspb.DistributionValue_BucketOptions{
								Type: &metricspb.DistributionValue_BucketOptions_Explicit_{
									Explicit: &metricspb.DistributionValue_BucketOptions_Explicit{
										Bounds: p.bounds,
									},
								},
							},
							Count:                 p.count,
							Sum:                   p.sum,
							Buckets:               buckets,
							SumOfSquaredDeviation: p.sumOfSquaredDeviation,
						},
					}
				}
			}
			timeseries[tidx] = &metricspb.TimeSeries{
				StartTimestamp: &timestamp.Timestamp{
					Seconds: ts.startTimestamp,
					Nanos:   0,
				},
				LabelValues: labelValues,
				Points:      points,
			}
		}

		// compose the metric
		md.Metrics[idx] = &metricspb.Metric{
			MetricDescriptor: &metricspb.MetricDescriptor{
				Name:      in.name,
				LabelKeys: labels,
			},
			Timeseries: timeseries,
		}
	}
	return md
}

func outLabelAggrBuilder(value int, isInt bool, isDouble bool) testMetric {
	outMetric := testMetric{
		name:      metric1,
		labelKeys: []string{label1},
		timeseries: []testTimeseries{
			{
				startTimestamp: 1,
				labelValues:    []string{labelValue11},
				points: []testPoint{
					{
						timestamp: 2,
						value:     value,
						isInt64:   isInt,
						isDouble:  isDouble,
					},
				},
			},
		},
	}
	return outMetric
}

func inLabelAggrBuilder(value1 int, value2 int, isInt bool, isDouble bool) testMetric {
	outMetric := testMetric{
		name:      metric1,
		labelKeys: []string{label1, label2},
		timeseries: []testTimeseries{
			{
				startTimestamp: 2,
				labelValues:    []string{labelValue11, labelValue21},
				points: []testPoint{
					{
						timestamp: 2,
						value:     value1,
						isInt64:   isInt,
						isDouble:  isDouble,
					},
				},
			},
			{
				startTimestamp: 1,
				labelValues:    []string{labelValue11, labelValue22},
				points: []testPoint{
					{
						timestamp: 2,
						value:     value2,
						isInt64:   isInt,
						isDouble:  isDouble,
					},
				},
			},
		},
	}
	return outMetric
}

func outLabelValuesAggrBuilder(value int, isInt bool, isDouble bool) testMetric {
	outMetric := testMetric{
		name:      metric1,
		labelKeys: []string{label1, label2},
		timeseries: []testTimeseries{
			{
				startTimestamp: 1,
				labelValues:    []string{newLabelValue11, labelValue21},
				points: []testPoint{
					{
						timestamp: 2,
						value:     value,
						isInt64:   isInt,
						isDouble:  isDouble,
					},
				},
			},
		},
	}
	return outMetric
}

func inLabelValuesAggrBuilder(value1 int, value2 int, isInt bool, isDouble bool) testMetric {
	outMetric := testMetric{
		name:      metric1,
		labelKeys: []string{label1, label2},
		timeseries: []testTimeseries{
			{
				startTimestamp: 2,
				labelValues:    []string{labelValue11, labelValue21},
				points: []testPoint{
					{
						timestamp: 2,
						value:     value1,
						isInt64:   isInt,
						isDouble:  isDouble,
					},
				},
			},
			{
				startTimestamp: 1,
				labelValues:    []string{labelValue12, labelValue21},
				points: []testPoint{
					{
						timestamp: 2,
						value:     value2,
						isInt64:   isInt,
						isDouble:  isDouble,
					},
				},
			},
		},
	}
	return outMetric
}

func distValueMetricBuilder(labels []string, labelValuesSet [][]string, startTimestamps []int64, pointsSet [][]testPoint) testMetric {
	timeseries := make([]testTimeseries, len(labelValuesSet))
	for i := 0; i < len(timeseries); i++ {
		points := make([]testPoint, len(pointsSet[i]))
		for j := 0; j < len(points); j++ {
			p := pointsSet[i][j]
			points[j] = testPoint{
				timestamp:             p.timestamp,
				value:                 p.value,
				count:                 p.count,
				sum:                   p.sum,
				bounds:                p.bounds,
				buckets:               p.buckets,
				sumOfSquaredDeviation: p.sumOfSquaredDeviation,
				isInt64:               false,
				isDouble:              false,
			}
		}

		timeseries[i] = testTimeseries{
			startTimestamp: startTimestamps[i],
			labelValues:    labelValuesSet[i],
			points:         points,
		}
	}

	outMetric := testMetric{
		name:       metric1,
		labelKeys:  labels,
		timeseries: timeseries,
	}
	return outMetric
}
