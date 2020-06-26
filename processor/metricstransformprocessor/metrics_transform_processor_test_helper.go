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
	newMetric1      = "metric1/new"
	newMetric2      = "metric2/new"
	newLabel1       = "label1/new"
	newLabelValue11 = "label1-value1/new"
	nonexist        = "nonexist"
)

type testTimeseries struct {
	startTimestamp int
	labelValues    []string
	points         struct {
		timestamp int
		value     int
	}
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
	}
)

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
		// construct timeseries with label values
		timeseries := make([]*metricspb.TimeSeries, len(in.timeseries))
		for tidx, ts := range in.timeseries {
			labelValues := make([]*metricspb.LabelValue, len(ts.labelValues))
			for vidx, value := range ts.labelValues {
				labelValues[vidx] = &metricspb.LabelValue{
					Value: value,
				}
			}
			timeseries[tidx] = &metricspb.TimeSeries{
				LabelValues: labelValues,
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
