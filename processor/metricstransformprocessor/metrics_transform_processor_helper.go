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
	"log"

	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
)

// update updates the original metric content in the metricPtr pointer
func (mtp *metricsTransformProcessor) update(metricPtr *metricspb.Metric) {
	// metric name update
	if mtp.newname != "" {
		metricPtr.MetricDescriptor.Name = mtp.newname
	}

	for _, op := range mtp.operations {
		// update label
		if op.Action == UpdateLabel {
			for idx, label := range metricPtr.MetricDescriptor.LabelKeys {
				if label.Key == op.Label {
					// label key update
					if op.NewLabel != "" {
						if mtp.validNewLabel(metricPtr.MetricDescriptor.LabelKeys, op.NewLabel) {
							label.Key = op.NewLabel
						} else {
							log.Printf("error running \"metrics_transform\" processor due to invalid \"new_label\": %v, which might be caused by a collision with existing label on metric named: %v", op.NewLabel, metricPtr.MetricDescriptor.Name)
						}
					}
					// label value update
					labelValuesMapping := mtp.createLabelValueMapping(op.ValueActions, metricPtr.Timeseries, idx)
					if len(labelValuesMapping) > 0 {
						for _, timeseries := range metricPtr.Timeseries {
							newValue, ok := labelValuesMapping[timeseries.LabelValues[idx].Value]
							if ok {
								timeseries.LabelValues[idx].Value = newValue
							}
						}
					}
				}
			}
		}
	}
}

// insert inserts a new copy of the metricPtr content into the metricPtrs slice
func (mtp *metricsTransformProcessor) insert(metricPtr *metricspb.Metric, metricPtrs []*metricspb.Metric) []*metricspb.Metric {
	metricCopy := mtp.createCopy(metricPtr)
	mtp.update(metricCopy)
	return append(metricPtrs, metricCopy)
}

// createCopy creates a new copy of the input metric
func (mtp *metricsTransformProcessor) createCopy(metricPtr *metricspb.Metric) *metricspb.Metric {
	copyMetricDescriptor := *metricPtr.MetricDescriptor
	copyLabelKeys := make([]*metricspb.LabelKey, 0)
	for _, labelKey := range copyMetricDescriptor.LabelKeys {
		copyLabelKeys = append(
			copyLabelKeys,
			&metricspb.LabelKey{
				Key:         labelKey.Key,
				Description: labelKey.Description,
			},
		)
	}
	copyMetricDescriptor.LabelKeys = copyLabelKeys

	copy := &metricspb.Metric{
		MetricDescriptor: &copyMetricDescriptor,
		Timeseries:       metricPtr.Timeseries,
		Resource:         metricPtr.Resource,
	}
	return copy
}

// createLabelValueMapping creates a label value mapping from old value to new value excluding the invalid ones
func (mtp *metricsTransformProcessor) createLabelValueMapping(valueActions []ValueAction, timeseries []*metricspb.TimeSeries, idx int) map[string]string {
	mapping := make(map[string]string)
	for _, valueAction := range valueActions {
		if mtp.validNewLabelValue(timeseries, idx, valueAction.NewValue) {
			mapping[valueAction.Value] = valueAction.NewValue
		} else {
			log.Printf("error running \"metrics_transform\" processor due to invalid \"new_value\": %v, which might be caused by a collision with existing label values", valueAction.NewValue)
		}
	}
	return mapping
}

// validNewName determines if the new name is a valid one. An invalid one is one that already exists
func (mtp *metricsTransformProcessor) validNewName(metricPtrs []*metricspb.Metric) bool {
	for _, metric := range metricPtrs {
		if metric.MetricDescriptor.Name == mtp.newname {
			return false
		}
	}
	return true
}

// validNewLabel determines if the new label is a valid one. An invalid one is one that already exists
func (mtp *metricsTransformProcessor) validNewLabel(labelKeys []*metricspb.LabelKey, newLabel string) bool {
	for _, label := range labelKeys {
		if label.Key == newLabel {
			return false
		}
	}
	return true
}

// validNewLabelValue determines if the new label value is a valid one. An invalid one is one that already exists
func (mtp *metricsTransformProcessor) validNewLabelValue(timeseries []*metricspb.TimeSeries, idx int, newValue string) bool {
	for _, t := range timeseries {
		if t.LabelValues[idx].Value == newValue {
			return false
		}
	}
	return true
}
