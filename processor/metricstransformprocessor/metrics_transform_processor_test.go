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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/consumerdata"
	"go.opentelemetry.io/collector/consumer/pdatautil"
	etest "go.opentelemetry.io/collector/exporter/exportertest"
)

func TestMetricsTransformProcessor(t *testing.T) {
	for _, test := range standardTests {
		t.Run(test.name, func(t *testing.T) {
			// next stores the results of the aggregation metric processor.
			next := &etest.SinkMetricsExporter{}
			cfg := &Config{
				ProcessorSettings: configmodels.ProcessorSettings{
					TypeVal: typeStr,
					NameVal: typeStr,
				},
				Transforms: test.transforms,
			}

			mtp, err := newMetricsTransformProcessor(next, cfg)
			assert.NotNil(t, mtp)
			assert.Nil(t, err)

			caps := mtp.GetCapabilities()
			assert.Equal(t, true, caps.MutatesConsumedData)
			ctx := context.Background()
			assert.NoError(t, mtp.Start(ctx, nil))

			// contruct metrics data to feed into the processor
			md := constructTestInputMetricsData(test)

			// process
			cErr := mtp.ConsumeMetrics(
				context.Background(),
				pdatautil.MetricsFromMetricsData([]consumerdata.MetricsData{
					md,
				}),
			)
			assert.Nil(t, cErr)

			// get and check results
			got := next.AllMetrics()
			require.Equal(t, 1, len(got))
			gotMD := pdatautil.MetricsToMetricsData(got[0])
			require.Equal(t, 1, len(gotMD))
			actualOut := gotMD[0].Metrics
			require.Equal(t, len(test.out), len(actualOut))

			for idx, out := range test.out {
				// name
				assert.Equal(t, out.name, actualOut[idx].MetricDescriptor.Name)
				// label keys
				for lidx, l := range out.labelKeys {
					assert.Equal(t, l, actualOut[idx].MetricDescriptor.LabelKeys[lidx].Key)
				}
				// label values
				for tidx, ts := range out.timeseries {
					for vidx, v := range ts.labelValues {
						assert.Equal(t, v, actualOut[idx].Timeseries[tidx].LabelValues[vidx].Value)
					}
				}
			}

			assert.NoError(t, mtp.Shutdown(ctx))
		})
	}
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
		next := &etest.SinkMetricsExporter{}
		cfg := &Config{
			ProcessorSettings: configmodels.ProcessorSettings{
				TypeVal: typeStr,
				NameVal: typeStr,
			},
			Transforms: test.transforms,
		}

		amp, err := newMetricsTransformProcessor(next, cfg)
		assert.NotNil(b, amp)
		assert.Nil(b, err)

		md := constructTestInputMetricsData(test)

		b.Run(test.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				assert.NoError(b, amp.ConsumeMetrics(
					context.Background(),
					pdatautil.MetricsFromMetricsData([]consumerdata.MetricsData{
						md,
					}),
				))
			}
		})
	}
}
