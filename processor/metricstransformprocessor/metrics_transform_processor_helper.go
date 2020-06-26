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
func (mtp *metricsTransformProcessor) update(metricPtr *metricspb.Metric, transform Transform) {
	// metric name update
	if transform.NewName != "" {
		metricPtr.MetricDescriptor.Name = transform.NewName
	}

	for _, op := range transform.Operations {
		// update label
		if op.Action == UpdateLabel {
			// if new_label is invalid, skip this operation
			if !mtp.validNewLabel(metricPtr.MetricDescriptor.LabelKeys, op.NewLabel) {
				log.Printf("error running %q processor due to collided %q: %v with existing label on metric named: %v detected by function %q", typeStr, NewLabelFieldName, op.NewLabel, metricPtr.MetricDescriptor.Name, validNewLabelFuncName)
				continue
			}
			for idx, label := range metricPtr.MetricDescriptor.LabelKeys {
				if label.Key == op.Label {
					// label key update
					if op.NewLabel != "" {
						label.Key = op.NewLabel
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
		// aggregate across labels
		// if op.Action == AggregateLabels {
		// 	// labelSet is a set of labels to keep
		// 	labelSet := make(map[string]bool)
		// 	for _, label := range op.LabelSet {
		// 		labelSet[label] = true
		// 	}
		// 	// labelIdxs is a slice containing the indices of the labels to keep. This is needed because label values are ordered in the same order as the labels
		// 	labelIdxs := make([]int, 0)
		// 	for idx, label := range metricPtr.MetricDescriptor.LabelKeys {
		// 		_, ok := labelSet[label.Key]
		// 		if ok {
		// 			labelIdxs = append(labelIdxs, idx)
		// 		}
		// 	}
		// 	// key is a copmosite of the label values as a single string
		// 	// keyToTimeseriesMap groups data points by the label values
		// 	keyToTimeseriesMap := make(map[string][]*metricspb.TimeSeries)
		// 	// keyToLabelValuesMap groups maps to the actual label values objects
		// 	keyToLabelValuesMap := make(map[string][]*metricspb.LabelValue)
		// 	for _, timeseries := range metricPtr.Timeseries {
		// 		var composedValues string
		// 		newLabelValues := make([]*metricspb.LabelValue, 0)
		// 		for _, vidx := range labelIdxs {
		// 			composedValues += timeseries.LabelValues[vidx].Value
		// 			newLabelValues = append(newLabelValues, timeseries.LabelValues[vidx])
		// 		}
		// 		timeseriesGroup, ok := keyToTimeseriesMap[composedValues]
		// 		if ok {
		// 			keyToTimeseriesMap[composedValues] = append(timeseriesGroup, timeseries)
		// 		} else {
		// 			keyToTimeseriesMap[composedValues] = []*metricspb.TimeSeries{timeseries}
		// 			keyToLabelValuesMap[composedValues] = newLabelValues
		// 		}
		// 	}
		// 	// iterate over the map to compose each group into one timeseries
		// 	newTimeSeries := make([]*metricspb.TimeSeries, 0)
		// 	for key, element := range keyToTimeseriesMap {
		// 		if (len(element) > 0) {
		// 			timestampToPointIdx := make(map[timestamp.Timestamp]int)
		// 			newPoints := make([]*metricspb.Point, 0)
		// 			newSingleTimeSeries := &metricspb.TimeSeries{
		// 				LabelValues: keyToLabelValuesMap[key],
		// 			}
		// 			for _, ts := range element {
		// 				for _, p := range ts.Points {
		// 					if idx, ok := timestampToPointIdx[*p.Timestamp]; ok {
		// 						newPoints[idx].Value += p.Value
		// 					} else {
		// 						timestampToPointIdx[*p.Timestamp] = len(newPoints)
		// 						newPoints = append(newPoints, p)
		// 					}
		// 				}
		// 			}
		// 		} else {

		// 		}
		// 	}
		// }
	}
}

// insert inserts a new copy of the metricPtr content into the metricPtrs slice.
// returns the new metrics list and the new metric
func (mtp *metricsTransformProcessor) insert(metricPtr *metricspb.Metric, metricPtrs []*metricspb.Metric, transform Transform) ([]*metricspb.Metric, *metricspb.Metric) {
	metricCopy := mtp.createCopy(metricPtr)
	mtp.update(metricCopy, transform)
	return append(metricPtrs, metricCopy), metricCopy
}

// createCopy creates a new copy of the input metric
func (mtp *metricsTransformProcessor) createCopy(metricPtr *metricspb.Metric) *metricspb.Metric {
	// copy Descriptor
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

	// copy Timeseries
	copyTimeseries := make([]*metricspb.TimeSeries, 0)
	for _, t := range metricPtr.Timeseries {
		copyLabelValues := make([]*metricspb.LabelValue, 0)
		for _, v := range t.LabelValues {
			copyLabelValues = append(
				copyLabelValues,
				&metricspb.LabelValue{
					Value:    v.Value,
					HasValue: v.HasValue,
				},
			)
		}
		copyTimeseries = append(
			copyTimeseries,
			&metricspb.TimeSeries{
				StartTimestamp: t.StartTimestamp,
				LabelValues:    copyLabelValues,
				Points:         t.Points,
			},
		)
	}

	copy := &metricspb.Metric{
		MetricDescriptor: &copyMetricDescriptor,
		Timeseries:       copyTimeseries,
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
			log.Printf("error running %q processor due to collided %q: %v with existing label values detected by function %q", typeStr, NewLabelValueFieldName, valueAction.NewValue, validNewLabelValueFuncName)
		}
	}
	return mapping
}

// validNewName determines if the new name is a valid one. An invalid one is one that already exists.
func (mtp *metricsTransformProcessor) validNewName(transform Transform, nameToMetricMapping map[string]*metricspb.Metric) bool {
	_, ok := nameToMetricMapping[transform.NewName]
	return !ok
}

// validNewLabel determines if the new label is a valid one. An invalid one is one that already exists.
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
