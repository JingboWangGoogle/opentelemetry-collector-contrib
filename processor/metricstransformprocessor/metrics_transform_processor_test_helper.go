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
	metricName     string
	action         ConfigAction
	newName        string
	operations     []Operation
	inMN           []string // input Metric names
	outMN          []string // output Metric names
	inLabels       []string
	outLabels      []string
	inLabelValues  [][]string
	outLabelValues [][]string
}

var (
	inMetricNames = []string{
		"metric1",
		"metric5",
	}

	outMetricNamesUpdate = []string{
		"metric1/new",
		"metric5",
	}

	outMetricNamesInsert = []string{
		"metric1",
		"metric5",
		"metric1/new",
	}

	inLabels = []string{
		"label1",
		"label2",
	}

	outLabels = []string{
		"label1/new",
		"label2",
	}

	inLabelValues = [][]string{
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
			name:       "metric_name_update",
			metricName: "metric1",
			action:     "update",
			newName:    "metric1/new",
			inMN:       inMetricNames,
			outMN:      outMetricNamesUpdate,
		},
		{
			name:       "metric_name_invalid_update",
			metricName: "metric1",
			action:     "update",
			newName:    "metric5",
			inMN:       inMetricNames,
			outMN:      inMetricNames,
		},
		{
			name:       "metric_label_update",
			metricName: "metric1",
			action:     "update",
			operations: []Operation{validUpateLabelOperation},
			inMN:       inMetricNames,
			outMN:      inMetricNames,
			inLabels:   inLabels,
			outLabels:  outLabels,
		},
		{
			name:       "metric_label_invalid_update",
			metricName: "metric1",
			action:     "update",
			operations: []Operation{invalidUpdateLabelOperation},
			inMN:       inMetricNames,
			outMN:      inMetricNames,
			inLabels:   inLabels,
			outLabels:  inLabels,
		},
		{
			name:           "metric_label_value_update",
			metricName:     "metric1",
			action:         "update",
			operations:     []Operation{validUpdateLabelValueOperation},
			inMN:           inMetricNames,
			outMN:          inMetricNames,
			inLabels:       inLabels,
			outLabels:      inLabels,
			inLabelValues:  inLabelValues,
			outLabelValues: outLabelValues,
		},
		{
			name:           "metric_label_value_invalid_update",
			metricName:     "metric1",
			action:         "update",
			operations:     []Operation{invalidUpdateLabelValueOperation},
			inMN:           inMetricNames,
			outMN:          inMetricNames,
			inLabels:       inLabels,
			outLabels:      inLabels,
			inLabelValues:  inLabelValues,
			outLabelValues: inLabelValues,
		},
		// INSERT
		{
			name:       "metric_name_insert",
			metricName: "metric1",
			action:     "insert",
			newName:    "metric1/new",
			inMN:       inMetricNames,
			outMN:      outMetricNamesInsert,
		},
		{
			name:       "metric_label_update_with_metric_insert",
			metricName: "metric1",
			action:     "insert",
			newName:    "metric1/new",
			operations: []Operation{validUpateLabelOperation},
			inMN:       inMetricNames,
			outMN:      outMetricNamesInsert,
			inLabels:   inLabels,
			outLabels:  outLabels,
		},
		{
			name:           "metric_label_value_update_with_metric_insert",
			metricName:     "metric1",
			action:         "insert",
			newName:        "metric1/new",
			operations:     []Operation{validUpdateLabelValueOperation},
			inMN:           inMetricNames,
			outMN:          outMetricNamesInsert,
			inLabels:       inLabels,
			outLabels:      inLabels,
			inLabelValues:  inLabelValues,
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
		if in == test.metricName {
			for _, l := range test.inLabels {
				labels = append(
					labels,
					&metricspb.LabelKey{
						Key: l,
					},
				)
			}
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
