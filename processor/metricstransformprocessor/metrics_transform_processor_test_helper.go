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

type metricsTransformTest struct {
	name           string // test name
	transforms     []Transform
	inMN           []string // input Metric names
	outMN          []string // output Metric names
	inLabels       []string
	outLabels      []string
	inLabelValues  [][]string
	outLabelValues [][]string
}

var (
	initialMetricNames = []string{
		"metric1",
		"metric5",
	}

	outMetricNamesUpdateSingle = []string{
		"metric1/new",
		"metric5",
	}
	outMetricNamesUpdateMultiple = []string{
		"metric1/new",
		"metric5/new",
	}

	outMetricNamesInsertSingle = []string{
		"metric1",
		"metric5",
		"metric1/new",
	}

	outMetricNamesInsertMultiple = []string{
		"metric1",
		"metric5",
		"metric1/new",
		"metric5/new",
	}

	initialLabels = []string{
		"label1",
		"label2",
	}

	outLabels = []string{
		"label1/new",
		"label2",
	}

	initialLabelValues = [][]string{
		{
			"label1-value1",
			"label2-value1",
		},
		{
			"label1-value2",
			"label2-value2",
		},
	}

	outLabelValues = [][]string{
		{
			"label1-value1/new",
			"label2-value1",
		},
		{
			"label1-value2",
			"label2-value2",
		},
	}

	validUpateLabelOperation = Operation{
		Action:   UpdateLabel,
		Label:    "label1",
		NewLabel: "label1/new",
	}

	invalidUpdateLabelOperation = Operation{
		Action:   UpdateLabel,
		Label:    "label1",
		NewLabel: "label2",
	}

	validUpdateLabelValueOperation = Operation{
		Action: UpdateLabel,
		Label:  "label1",
		ValueActions: []ValueAction{
			{
				Value:    "label1-value1",
				NewValue: "label1-value1/new",
			},
		},
	}

	invalidUpdateLabelValueOperation = Operation{
		Action: UpdateLabel,
		Label:  "label1",
		ValueActions: []ValueAction{
			{
				Value:    "label1-value1",
				NewValue: "label1-value2",
			},
		},
	}

	standardTests = []metricsTransformTest{
		// UPDATE
		{
			name: "metric_name_update",
			transforms: []Transform{
				{
					MetricName: "metric1",
					Action:     Update,
					NewName:    "metric1/new",
				},
			},
			inMN:  initialMetricNames,
			outMN: outMetricNamesUpdateSingle,
		},
		{
			name: "metric_name_update_multiple",
			transforms: []Transform{
				{
					MetricName: "metric1",
					Action:     Update,
					NewName:    "metric1/new",
				},
				{
					MetricName: "metric5",
					Action:     Update,
					NewName:    "metric5/new",
				},
			},
			inMN:  initialMetricNames,
			outMN: outMetricNamesUpdateMultiple,
		},
		{
			name: "metric_name_update_nonexist",
			transforms: []Transform{
				{
					MetricName: "metric100",
					Action:     Update,
					NewName:    "metric1/new",
				},
			},
			inMN:  initialMetricNames,
			outMN: initialMetricNames,
		},
		{
			name: "metric_name_update_invalid",
			transforms: []Transform{
				{
					MetricName: "metric1",
					Action:     Update,
					NewName:    "metric5",
				},
			},
			inMN:  initialMetricNames,
			outMN: initialMetricNames,
		},
		{
			name: "metric_label_update",
			transforms: []Transform{
				{
					MetricName: "metric1",
					Action:     Update,
					Operations: []Operation{validUpateLabelOperation},
				},
			},
			inMN:      initialMetricNames,
			outMN:     initialMetricNames,
			inLabels:  initialLabels,
			outLabels: outLabels,
		},
		{
			name: "metric_label_update_invalid",
			transforms: []Transform{
				{
					MetricName: "metric1",
					Action:     Update,
					Operations: []Operation{invalidUpdateLabelOperation},
				},
			},
			inMN:      initialMetricNames,
			outMN:     initialMetricNames,
			inLabels:  initialLabels,
			outLabels: initialLabels,
		},
		{
			name: "metric_label_value_update",
			transforms: []Transform{
				{
					MetricName: "metric1",
					Action:     Update,
					Operations: []Operation{validUpdateLabelValueOperation},
				},
			},
			inMN:           initialMetricNames,
			outMN:          initialMetricNames,
			inLabels:       initialLabels,
			outLabels:      initialLabels,
			inLabelValues:  initialLabelValues,
			outLabelValues: outLabelValues,
		},
		{
			name: "metric_label_value_update_invalid",
			transforms: []Transform{
				{
					MetricName: "metric1",
					Action:     Update,
					Operations: []Operation{invalidUpdateLabelValueOperation},
				},
			},
			inMN:           initialMetricNames,
			outMN:          initialMetricNames,
			inLabels:       initialLabels,
			outLabels:      initialLabels,
			inLabelValues:  initialLabelValues,
			outLabelValues: initialLabelValues,
		},
		// INSERT
		{
			name: "metric_name_insert",
			transforms: []Transform{
				{
					MetricName: "metric1",
					Action:     Insert,
					NewName:    "metric1/new",
				},
			},
			inMN:  initialMetricNames,
			outMN: outMetricNamesInsertSingle,
		},
		{
			name: "metric_name_insert_multiple",
			transforms: []Transform{
				{
					MetricName: "metric1",
					Action:     Insert,
					NewName:    "metric1/new",
				},
				{
					MetricName: "metric5",
					Action:     Insert,
					NewName:    "metric5/new",
				},
			},
			inMN:  initialMetricNames,
			outMN: outMetricNamesInsertMultiple,
		},
		{
			name: "metric_label_update_with_metric_insert",
			transforms: []Transform{
				{
					MetricName: "metric1",
					Action:     Insert,
					NewName:    "metric1/new",
					Operations: []Operation{validUpateLabelOperation},
				},
			},
			inMN:      initialMetricNames,
			outMN:     outMetricNamesInsertSingle,
			inLabels:  initialLabels,
			outLabels: outLabels,
		},
		{
			name: "metric_label_value_update_with_metric_insert",
			transforms: []Transform{
				{
					MetricName: "metric1",
					Action:     Insert,
					NewName:    "metric1/new",
					Operations: []Operation{validUpdateLabelValueOperation},
				},
			},
			inMN:           initialMetricNames,
			outMN:          outMetricNamesInsertSingle,
			inLabels:       initialLabels,
			outLabels:      initialLabels,
			inLabelValues:  initialLabelValues,
			outLabelValues: outLabelValues,
		},
	}
)

func constructTestInputMetricsData(test metricsTransformTest) consumerdata.MetricsData {
	md := consumerdata.MetricsData{
		Metrics: make([]*metricspb.Metric, len(test.inMN)),
	}
	for idx, in := range test.inMN {
		// construct label keys
		labels := make([]*metricspb.LabelKey, 0)
		for _, l := range test.inLabels {
			labels = append(
				labels,
				&metricspb.LabelKey{
					Key: l,
				},
			)
		}
		// construct label values
		labelValueSets := make([][]*metricspb.LabelValue, 0)
		for _, values := range test.inLabelValues {
			labelValueSet := make([]*metricspb.LabelValue, 0)
			for _, value := range values {
				labelValueSet = append(
					labelValueSet,
					&metricspb.LabelValue{
						Value: value,
					},
				)
			}
			labelValueSets = append(labelValueSets, labelValueSet)
		}
		timeseries := make([]*metricspb.TimeSeries, 0)
		for _, valueSet := range labelValueSets {
			timeseries = append(
				timeseries,
				&metricspb.TimeSeries{
					LabelValues: valueSet,
				},
			)
		}

		// compose the metric
		md.Metrics[idx] = &metricspb.Metric{
			MetricDescriptor: &metricspb.MetricDescriptor{
				Name:      in,
				LabelKeys: labels,
			},
			Timeseries: timeseries,
		}
	}
	return md
}
