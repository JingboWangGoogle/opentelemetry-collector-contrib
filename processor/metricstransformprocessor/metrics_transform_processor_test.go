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
	"context"
	"testing"

	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/consumerdata"
	"go.opentelemetry.io/collector/consumer/pdatautil"
	etest "go.opentelemetry.io/collector/exporter/exportertest"
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

func TestMetricsTransformProcessor(t *testing.T) {
	for _, test := range standardTests {
		t.Run(test.name, func(t *testing.T) {
			// next stores the results of the aggregation metric processor
			next := &etest.SinkMetricsExporter{}
			cfg := &Config{
				ProcessorSettings: configmodels.ProcessorSettings{
					TypeVal: typeStr,
					NameVal: typeStr,
				},
				MetricName: test.metricName,
				Action:     test.action,
				NewName:    test.newName,
				Operations: test.operations,
			}
			amp, err := newMetricsTransformProcessor(next, cfg)
			assert.NotNil(t, amp)
			assert.Nil(t, err)

			caps := amp.GetCapabilities()
			assert.Equal(t, false, caps.MutatesConsumedData)
			ctx := context.Background()
			assert.NoError(t, amp.Start(ctx, nil))

			md := consumerdata.MetricsData{
				Metrics: make([]*metricspb.Metric, len(test.inMN)),
			}

			// contruct metrics data to feed into the processor
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
				// TODO: move these to helper functions
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

			// process
			cErr := amp.ConsumeMetrics(
				context.Background(),
				pdatautil.MetricsFromMetricsData([]consumerdata.MetricsData{
					{},
					md,
					{
						Metrics: []*metricspb.Metric{},
					},
				}),
			)
			assert.Nil(t, cErr)

			// get and check results
			got := next.AllMetrics()
			require.Equal(t, 1, len(got))
			gotMD := pdatautil.MetricsToMetricsData(got[0])
			require.Equal(t, 3, len(gotMD))
			assert.EqualValues(t, consumerdata.MetricsData{}, gotMD[0])
			require.Equal(t, len(test.outMN), len(gotMD[1].Metrics))

			targetName := test.metricName
			if test.newName != "" {
				targetName = test.newName
			}
			for idx, out := range gotMD[1].Metrics {
				// check name
				assert.Equal(t, test.outMN[idx], out.MetricDescriptor.Name)
				// check labels
				// check the updated or inserted is correctly updated
				if out.MetricDescriptor.Name == targetName {
					for lidx, l := range out.MetricDescriptor.LabelKeys {
						assert.Equal(t, test.outLabels[lidx], l.Key)
					}
					for tidx, ts := range out.Timeseries {
						for lidx, v := range ts.LabelValues {
							assert.Equal(t, test.outLabelValues[tidx][lidx], v.Value)
						}
					}
				}
				// check the original is untouched if insert
				if test.action == Insert {
					if out.MetricDescriptor.Name == test.metricName {
						for lidx, l := range out.MetricDescriptor.LabelKeys {
							assert.Equal(t, test.inLabels[lidx], l.Key)
						}
						for tidx, ts := range out.Timeseries {
							for lidx, v := range ts.LabelValues {
								assert.Equal(t, test.inLabelValues[tidx][lidx], v.Value)
							}
						}
					}
				}
			}

			assert.EqualValues(t, consumerdata.MetricsData{Metrics: []*metricspb.Metric{}}, gotMD[2])
			assert.NoError(t, amp.Shutdown(ctx))
		})
	}
}

func BenchmarkMetricsTransformProcessorRenameMetrics(b *testing.B) {
	// runs 1000 metrics through a filterprocessor with both include and exclude filters.
	stressTest := metricsTransformTest{
		name:       "1000Metrics",
		metricName: "metric1",
		action:     "insert",
		newName:    "newname",
	}

	for len(stressTest.inMN) < 1000 {
		stressTest.inMN = append(stressTest.inMN, inMetricNames...)
	}

	benchmarkTests := append(standardTests, stressTest)

	for _, test := range benchmarkTests {
		// next stores the results of the filter metric processor
		next := &etest.SinkMetricsExporter{}
		cfg := &Config{
			ProcessorSettings: configmodels.ProcessorSettings{
				TypeVal: typeStr,
				NameVal: typeStr,
			},
			MetricName: test.metricName,
			Action:     test.action,
			NewName:    test.newName,
		}

		amp, err := newMetricsTransformProcessor(next, cfg)
		assert.NotNil(b, amp)
		assert.Nil(b, err)

		md := consumerdata.MetricsData{
			Metrics: make([]*metricspb.Metric, len(test.inMN)),
		}

		for idx, in := range test.inMN {
			md.Metrics[idx] = &metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name: in,
				},
			}
		}

		b.Run(test.metricName, func(b *testing.B) {
			assert.NoError(b, amp.ConsumeMetrics(
				context.Background(),
				pdatautil.MetricsFromMetricsData([]consumerdata.MetricsData{
					{},
					md,
					{
						Metrics: []*metricspb.Metric{},
					},
				}),
			))
		})
	}
}
