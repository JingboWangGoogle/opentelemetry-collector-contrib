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
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
)

type builder struct {
	testcase *metricspb.Metric
}

// TestcaseBuilder is used to build metrics testcases
func TestcaseBuilder() builder {
	return builder{
		testcase: &metricspb.Metric{
			MetricDescriptor: &metricspb.MetricDescriptor{},
			Timeseries:       make([]*metricspb.TimeSeries, 0),
		},
	}
}

// SetName sets the name for the testcase
func (b builder) SetName(name string) builder {
	b.testcase.MetricDescriptor.Name = name
	return b
}

// SetLabels sets the labels for the testcase
func (b builder) SetLabels(labels []string) builder {
	labelKeys := make([]*metricspb.LabelKey, len(labels))
	for i, l := range labels {
		labelKeys[i] = &metricspb.LabelKey{
			Key: l,
		}
	}
	b.testcase.MetricDescriptor.LabelKeys = labelKeys
	return b
}

// AddTimeseries adds new timeseries with the labelValuesVal and startTimestamp
func (b builder) AddTimeseries(startTimestamp int64, labelValuesVal []string) builder {
	labelValues := make([]*metricspb.LabelValue, len(labelValuesVal))
	for i, v := range labelValuesVal {
		labelValues[i] = &metricspb.LabelValue{
			Value: v,
		}
	}
	timeseries := &metricspb.TimeSeries{
		StartTimestamp: &timestamp.Timestamp{
			Seconds: startTimestamp,
			Nanos:   0,
		},
		LabelValues: labelValues,
		Points:      make([]*metricspb.Point, 0),
	}
	b.testcase.Timeseries = append(b.testcase.Timeseries, timeseries)
	return b
}

// SetDataType sets the data type of this metric
func (b builder) SetDataType(dataType metricspb.MetricDescriptor_Type) builder {
	b.testcase.MetricDescriptor.Type = dataType
	return b
}

// AddInt64Point adds a int64 point to the tidx-th timseries
func (b builder) AddInt64Point(tidx int, val int64, timestampVal int64) builder {
	point := &metricspb.Point{
		Timestamp: &timestamp.Timestamp{
			Seconds: timestampVal,
			Nanos:   0,
		},
		Value: &metricspb.Point_Int64Value{
			Int64Value: val,
		},
	}
	points := b.testcase.Timeseries[tidx].Points
	b.testcase.Timeseries[tidx].Points = append(points, point)
	return b
}

// AddDoublePoint adds a double point to the tidx-th timseries
func (b builder) AddDoublePoint(tidx int, val float64, timestampVal int64) builder {
	point := &metricspb.Point{
		Timestamp: &timestamp.Timestamp{
			Seconds: timestampVal,
			Nanos:   0,
		},
		Value: &metricspb.Point_DoubleValue{
			DoubleValue: val,
		},
	}
	points := b.testcase.Timeseries[tidx].Points
	b.testcase.Timeseries[tidx].Points = append(points, point)
	return b
}

// AddDistributionPoints adds a distribution point to the tidx-th timseries
func (b builder) AddDistributionPoints(tidx int, timestampVal int64, count int64, sum float64, bounds []float64, bucketsVal []int64, sumOfSquaredDeviation float64) builder {
	buckets := make([]*metricspb.DistributionValue_Bucket, len(bucketsVal))
	for buIdx, bucket := range bucketsVal {
		buckets[buIdx] = &metricspb.DistributionValue_Bucket{
			Count: bucket,
		}
	}
	point := &metricspb.Point{
		Timestamp: &timestamp.Timestamp{
			Seconds: timestampVal,
			Nanos:   0,
		},
		Value: &metricspb.Point_DistributionValue{
			DistributionValue: &metricspb.DistributionValue{
				BucketOptions: &metricspb.DistributionValue_BucketOptions{
					Type: &metricspb.DistributionValue_BucketOptions_Explicit_{
						Explicit: &metricspb.DistributionValue_BucketOptions_Explicit{
							Bounds: bounds,
						},
					},
				},
				Count:                 count,
				Sum:                   sum,
				Buckets:               buckets,
				SumOfSquaredDeviation: sumOfSquaredDeviation,
			},
		},
	}
	points := b.testcase.Timeseries[tidx].Points
	b.testcase.Timeseries[tidx].Points = append(points, point)
	return b
}

// Build builds from the builder to the final metric
func (b builder) Build() *metricspb.Metric {
	return b.testcase
}
