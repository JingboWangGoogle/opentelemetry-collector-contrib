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
	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/consumerdata"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/consumer/pdatautil"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"testing"
)

// Toggle Data Type
// {
// 	name: "metric_toggle_scalar_data_type_int64_to_double",
// 	transforms: []Transform{
// 		{
// 			MetricName: "metric1",
// 			Action:     Update,
// 			Operations: []Operation{{Action: ToggleScalarDataType}},
// 		},
// 		{
// 			MetricName: "metric2",
// 			Action:     Update,
// 			Operations: []Operation{{Action: ToggleScalarDataType}},
// 		},
// 	},
// 	inMetrics: []*metricspb.MetricDescriptor{
// 		{Name: "metric1", Type: metricspb.MetricDescriptor_CUMULATIVE_INT64},
// 		{Name: "metric2", Type: metricspb.MetricDescriptor_GAUGE_INT64},
// 	},
// 	outMetrics: []*metricspb.MetricDescriptor{
// 		{Name: "metric1", Type: metricspb.MetricDescriptor_CUMULATIVE_DOUBLE},
// 		{Name: "metric2", Type: metricspb.MetricDescriptor_GAUGE_DOUBLE},
// 	},
// },
// {
// 	name: "metric_toggle_scalar_data_type_double_to_int64",
// 	transforms: []Transform{
// 		{
// 			MetricName: "metric1",
// 			Action:     Update,
// 			Operations: []Operation{{Action: ToggleScalarDataType}},
// 		},
// 		{
// 			MetricName: "metric2",
// 			Action:     Update,
// 			Operations: []Operation{{Action: ToggleScalarDataType}},
// 		},
// 	},
// 	inMetrics: []*metricspb.MetricDescriptor{
// 		{Name: "metric1", Type: metricspb.MetricDescriptor_CUMULATIVE_DOUBLE},
// 		{Name: "metric2", Type: metricspb.MetricDescriptor_GAUGE_DOUBLE},
// 	},
// 	outMetrics: []*metricspb.MetricDescriptor{
// 		{Name: "metric1", Type: metricspb.MetricDescriptor_CUMULATIVE_INT64},
// 		{Name: "metric2", Type: metricspb.MetricDescriptor_GAUGE_INT64},
// 	},
// },
// {
// 	name: "metric_toggle_scalar_data_type_no_effect",
// 	transforms: []Transform{
// 		{
// 			MetricName: "metric1",
// 			Action:     Update,
// 			Operations: []Operation{{Action: ToggleScalarDataType}},
// 		},
// 	},
// 	inMetrics:  []*metricspb.MetricDescriptor{{Name: "metric1", Type: metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION}},
// 	outMetrics: []*metricspb.MetricDescriptor{{Name: "metric1", Type: metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION}},
// },
// // Add Label to a metric
// {
// 	name: "update existing metric by adding a new label when there are no labels",
// 	transforms: []Transform{
// 		{
// 			MetricName: "metric1",
// 			Action:     Update,
// 			Operations: []Operation{
// 				{
// 					Action:   AddLabel,
// 					NewLabel: "foo",
// 					NewValue: "bar",
// 				},
// 			},
// 		},
// 	},
// 	inMetrics:  initialMetricNames,
// 	outMetrics: initialMetricNames,
// 	outLabels:  []labelKeyValue{{Key: "foo", Value: "bar"}},
// },
// {
// 	name: "update existing metric by adding a new label when there are labels",
// 	transforms: []Transform{
// 		{
// 			MetricName: "metric1",
// 			Action:     Update,
// 			Operations: []Operation{
// 				{
// 					Action:   AddLabel,
// 					NewLabel: "foo",
// 					NewValue: "bar",
// 				},
// 			},
// 		},
// 	},
// 	inMetrics:  initialMetricNames,
// 	outMetrics: initialMetricNames,
// 	inLabels:   initialLabels,
// 	outLabels: []labelKeyValue{
// 		{
// 			Key:   "label1",
// 			Value: "value1",
// 		},
// 		{
// 			Key:   "label2",
// 			Value: "value2",
// 		},
// 		{
// 			Key:   "foo",
// 			Value: "bar",
// 		},
// 	},
// },
// {
// 	name: "update existing metric by adding a label that is duplicated in the list",
// 	transforms: []Transform{
// 		{
// 			MetricName: "metric1",
// 			Action:     Update,
// 			Operations: []Operation{
// 				{
// 					Action:   AddLabel,
// 					NewLabel: "label1",
// 					NewValue: "value1",
// 				},
// 			},
// 		},
// 	},
// 	inMetrics:  initialMetricNames,
// 	outMetrics: initialMetricNames,
// 	inLabels:   initialLabels,
// 	outLabels: []labelKeyValue{
// 		{
// 			Key:   "label1",
// 			Value: "value1",
// 		},
// 		{
// 			Key:   "label2",
// 			Value: "value2",
// 		},
// 		{
// 			Key:   "label1",
// 			Value: "value1",
// 		},
// 	},
// },
// {
// 	name: "update does not happen because target metric doesn't exist",
// 	transforms: []Transform{
// 		{
// 			MetricName: "mymetric",
// 			Action:     Update,
// 			Operations: []Operation{
// 				{
// 					Action:   AddLabel,
// 					NewLabel: "foo",
// 					NewValue: "bar",
// 				},
// 			},
// 		},
// 	},
// 	inMetrics:  initialMetricNames,
// 	outMetrics: initialMetricNames,
// 	inLabels:   initialLabels,
// 	outLabels:  initialLabels,
// },
func TestMetricsTransformProcessor(t *testing.T) {
	for _, test := range standardTests {
		t.Run(test.name, func(t *testing.T) {
			next := &exportertest.SinkMetricsExporter{}
			cfg := &Config{Transforms: test.transforms}

			mtp := newMetricsTransformProcessor(next, cfg)
			assert.True(t, mtp.GetCapabilities().MutatesConsumedData)
			assert.NoError(t, mtp.Start(context.Background(), componenttest.NewNopHost()))
			defer func() { assert.NoError(t, mtp.Shutdown(context.Background())) }()

			inputMetrics := createTestMetrics(test.inMetrics, test.inLabels)
			assert.NoError(t, mtp.ConsumeMetrics(context.Background(), inputMetrics))

			// contruct metrics data to feed into the processor
			md := constructTestInputMetricsData(test)

			// process
			cErr := mtp.ConsumeMetrics(
				context.Background(),
				pdatautil.MetricsFromMetricsData([]consumerdata.MetricsData{
					md,
				}),
			)
			assert.NoError(t, cErr)

			// get and check results
			got := next.AllMetrics()
			require.Equal(t, 1, len(got))
			gotMD := pdatautil.MetricsToMetricsData(got[0])
			require.Equal(t, 1, len(gotMD))
			actualOut := gotMD[0].Metrics
			require.Equal(t, len(test.out), len(actualOut))

			for idx, out := range test.out {
				actualDescriptor := actualOut[idx].MetricDescriptor
				// name
				assert.Equal(t, out.name, actualDescriptor.Name)
				// label keys
				assert.Equal(t, len(out.labelKeys), len(actualDescriptor.LabelKeys))
				for lidx, l := range out.labelKeys {
					assert.Equal(t, l, actualDescriptor.LabelKeys[lidx].Key)
				}
				// timeseries
				assert.Equal(t, len(out.timeseries), len(actualOut[idx].Timeseries))
				for tidx, ts := range out.timeseries {
					actualTimeseries := actualOut[idx].Timeseries[tidx]
					// start timestamp
					assert.Equal(t, ts.startTimestamp, actualTimeseries.StartTimestamp.Seconds)
					// label values
					assert.Equal(t, len(ts.labelValues), len(actualTimeseries.LabelValues))
					for vidx, v := range ts.labelValues {
						assert.Equal(t, v, actualTimeseries.LabelValues[vidx].Value)
					}
					// points
					assert.Equal(t, len(ts.points), len(actualTimeseries.Points))
					for pidx, p := range ts.points {
						actualPoint := actualTimeseries.Points[pidx]
						assert.Equal(t, p.timestamp, actualPoint.Timestamp.Seconds)
						if p.isInt64 {
							assert.Equal(t, int64(p.value), actualPoint.GetInt64Value())
						} else if p.isDouble {
							assert.Equal(t, float64(p.value), actualPoint.GetDoubleValue())
						} else {
							assert.Equal(t, p.sum, actualPoint.GetDistributionValue().GetSum())
							assert.Equal(t, p.count, actualPoint.GetDistributionValue().GetCount())
							assert.Equal(t, p.sumOfSquaredDeviation, actualPoint.GetDistributionValue().GetSumOfSquaredDeviation())
							for boIdx, bound := range p.bounds {
								assert.Equal(t, bound, actualPoint.GetDistributionValue().GetBucketOptions().GetExplicit().Bounds[boIdx])
							}
							for buIdx, bucket := range p.buckets {
								assert.Equal(t, bucket, actualPoint.GetDistributionValue().GetBuckets()[buIdx].Count)
							}
						}
					}
				}
			}
		})
	}
}

func validateLabels(t *testing.T, expectLabels []labelKeyValue, inMetric *metricspb.Metric) {
	assert.Equal(t, len(expectLabels), len(inMetric.MetricDescriptor.LabelKeys))
	for lidx, l := range inMetric.MetricDescriptor.LabelKeys {
		assert.Equal(t, expectLabels[lidx].Key, l.Key)
	}
	for _, ts := range inMetric.Timeseries {
		assert.Equal(t, len(expectLabels), len(ts.LabelValues))
		for lidx, l := range ts.LabelValues {
			assert.Equal(t, expectLabels[lidx].Value, l.Value)
			assert.True(t, l.HasValue)
		}
	}
}

func createTestMetrics(inMetrics []*metricspb.MetricDescriptor, labelKV []labelKeyValue) pdata.Metrics {
	md := consumerdata.MetricsData{
		Metrics: make([]*metricspb.Metric, len(inMetrics)),
	}

	for i, inMetric := range inMetrics {
		descriptor := proto.Clone(inMetric).(*metricspb.MetricDescriptor)

		labelKeys := make([]*metricspb.LabelKey, len(labelKV))
		labelValues := make([]*metricspb.LabelValue, len(labelKV))
		for j, label := range labelKV {
			labelKeys[j] = &metricspb.LabelKey{Key: label.Key}
			labelValues[j] = &metricspb.LabelValue{Value: label.Value, HasValue: true}
		}
		descriptor.LabelKeys = labelKeys

		md.Metrics[i] = &metricspb.Metric{
			MetricDescriptor: descriptor,
			Timeseries: []*metricspb.TimeSeries{
				{
					Points: []*metricspb.Point{
						{
							Value: &metricspb.Point_Int64Value{Int64Value: 1},
						}, {
							Value: &metricspb.Point_DoubleValue{DoubleValue: 2},
						},
					},
					LabelValues: labelValues,
				},
			},
		}
	}

	return pdatautil.MetricsFromMetricsData([]consumerdata.MetricsData{md})
}

func BenchmarkMetricsTransformProcessorRenameMetrics(b *testing.B) {
	// runs 1000 metrics through a filterprocessor with both include and exclude filters.
	stressTest := metricsTransformTest{
		name: "1000Metrics",
		transforms: []Transform{
			{
				MetricName: metric1,
				Action:     Insert,
				NewName:    newMetric1,
			},
		},
	}

	for len(stressTest.in) < 1000 {
		stressTest.in = append(stressTest.in, initialMetricRename1)
	}

	benchmarkTests := []metricsTransformTest{stressTest}

	for _, test := range benchmarkTests {
		// next stores the results of the filter metric processor.
		next := &exportertest.SinkMetricsExporter{}
		cfg := &Config{
			ProcessorSettings: configmodels.ProcessorSettings{
				TypeVal: typeStr,
				NameVal: typeStr,
			},
			Transforms: test.transforms,
		}

		mtp := newMetricsTransformProcessor(next, cfg)
		assert.NotNil(b, mtp)

		md := constructTestInputMetricsData(test)

		b.Run(test.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				assert.NoError(b, mtp.ConsumeMetrics(
					context.Background(),
					pdatautil.MetricsFromMetricsData([]consumerdata.MetricsData{
						md,
					}),
				))
			}
		})
	}
}
