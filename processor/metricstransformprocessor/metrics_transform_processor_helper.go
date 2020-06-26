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
		if op.Action == AggregateLabels {
			// labelSet is a set of labels to keep
			labelSet := mtp.sliceToSet(op.LabelSet)
			// labelIdxs is a slice containing the indices of the labels to keep. This is needed because label values are ordered in the same order as the labels
			labelIdxs, labels := mtp.getLabelIdxs(metricPtr, labelSet)
			// key is a composite of the label values as a single string
			// keyToTimeseriesMap groups timeseries by the label values
			// keyToLabelValuesMap groups the actual label values objects
			keyToTimeseriesMap, keyToLabelValuesMap := mtp.constructAggrGroupsMaps(metricPtr, labelIdxs)

			// compose groups into timeseries
			newTimeseries := mtp.composeTimeseriesGroups(keyToTimeseriesMap, keyToLabelValuesMap, op.AggregationType)

			metricPtr.Timeseries = newTimeseries
			metricPtr.MetricDescriptor.LabelKeys = labels
		}
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

func (mtp *metricsTransformProcessor) sliceToSet(slice []string) map[string]bool {
	set := make(map[string]bool)
	for _, label := range slice {
		set[label] = true
	}
	return set
}

func (mtp *metricsTransformProcessor) getLabelIdxs(metricPtr *metricspb.Metric, labelSet map[string]bool) ([]int, []*metricspb.LabelKey) {
	labelIdxs := make([]int, 0)
	labels := make([]*metricspb.LabelKey, 0)
	for idx, label := range metricPtr.MetricDescriptor.LabelKeys {
		_, ok := labelSet[label.Key]
		if ok {
			labelIdxs = append(labelIdxs, idx)
			labels = append(labels, label)
		}
	}
	return labelIdxs, labels
}

func (mtp *metricsTransformProcessor) constructAggrGroupsMaps(metricPtr *metricspb.Metric, labelIdxs []int) (map[string][]*metricspb.TimeSeries, map[string][]*metricspb.LabelValue) {
	// key is a composite of the label values as a single string
	// keyToTimeseriesMap groups timeseries by the label values
	keyToTimeseriesMap := make(map[string][]*metricspb.TimeSeries)
	// keyToLabelValuesMap groups the actual label values objects
	keyToLabelValuesMap := make(map[string][]*metricspb.LabelValue)
	for _, timeseries := range metricPtr.Timeseries {
		// composedValues is the key, the composite of the label values as a single string
		var composedValues string
		// newLabelValues are the label values after aggregation, dropping the excluded ones
		newLabelValues := make([]*metricspb.LabelValue, len(labelIdxs))
		for i, vidx := range labelIdxs {
			composedValues += timeseries.LabelValues[vidx].Value
			newLabelValues[i] = timeseries.LabelValues[vidx]
		}
		timeseriesGroup, ok := keyToTimeseriesMap[composedValues]
		if ok {
			keyToTimeseriesMap[composedValues] = append(timeseriesGroup, timeseries)
		} else {
			keyToTimeseriesMap[composedValues] = []*metricspb.TimeSeries{timeseries}
			keyToLabelValuesMap[composedValues] = newLabelValues
		}
	}
	return keyToTimeseriesMap, keyToLabelValuesMap
}

func (mtp *metricsTransformProcessor) composeTimeseriesGroups(keyToTimeseriesMap map[string][]*metricspb.TimeSeries, keyToLabelValuesMap map[string][]*metricspb.LabelValue, aggrType AggregationType) []*metricspb.TimeSeries {
	newTimeSeries := make([]*metricspb.TimeSeries, len(keyToTimeseriesMap))
	idxCounter := 0
	for key, element := range keyToTimeseriesMap {
		startTimestamp := element[0].StartTimestamp
		// timestampToPoints maps from timestamp string to points
		timestampToPoints := make(map[string][]*metricspb.Point)
		for _, ts := range element {
			if ts.StartTimestamp.Seconds < startTimestamp.Seconds || (ts.StartTimestamp.Seconds == startTimestamp.Seconds && ts.StartTimestamp.Nanos < startTimestamp.Nanos) {
				startTimestamp = ts.StartTimestamp
			}
			for _, p := range ts.Points {
				if points, ok := timestampToPoints[p.Timestamp.String()]; ok {
					timestampToPoints[p.Timestamp.String()] = append(points, p)
				} else {
					timestampToPoints[p.Timestamp.String()] = []*metricspb.Point{p}
				}
			}
		}
		newPoints := make([]*metricspb.Point, len(timestampToPoints))
		pidxCounter := 0
		for _, points := range timestampToPoints {
			newPoints[pidxCounter] = &metricspb.Point{
				Timestamp: points[0].Timestamp,
			}
			intPoint, doublePoint := mtp.compute(points, aggrType)
			if intPoint != nil {
				newPoints[pidxCounter].Value = intPoint
			} else if doublePoint != nil {
				newPoints[pidxCounter].Value = doublePoint
			} else {
				newPoints[pidxCounter].Value = points[0].Value
			}
			pidxCounter++
		}
		// newSingleTimeSeries is an aggregated timeseries
		newSingleTimeSeries := &metricspb.TimeSeries{
			StartTimestamp: startTimestamp,
			LabelValues:    keyToLabelValuesMap[key],
			Points:         newPoints,
		}
		newTimeSeries[idxCounter] = newSingleTimeSeries
		idxCounter++
	}

	return newTimeSeries
}

func (mtp *metricsTransformProcessor) compute(points []*metricspb.Point, aggrType AggregationType) (*metricspb.Point_Int64Value, *metricspb.Point_DoubleValue) {
	intVal := int64(0)
	doubleVal := float64(0)
	if points[0].GetInt64Value() != 0 {
		for _, p := range points {
			if aggrType == Sum || aggrType == Average {
				intVal += p.GetInt64Value()
			} else if aggrType == Max {
				if p.GetInt64Value() <= intVal {
					continue
				}
				intVal = p.GetInt64Value()
			}

		}
		if aggrType == Average {
			intVal /= int64(len(points))
		}
		return &metricspb.Point_Int64Value{Int64Value: intVal}, nil
	} else if points[0].GetDoubleValue() != 0 {
		for _, p := range points {
			if aggrType == Sum || aggrType == Average {
				doubleVal += p.GetDoubleValue()
			} else if aggrType == Max {
				if p.GetDoubleValue() <= doubleVal {
					continue
				}
				doubleVal = p.GetDoubleValue()
			}

		}
		if aggrType == Average {
			doubleVal /= float64(len(points))
		}
		return nil, &metricspb.Point_DoubleValue{DoubleValue: doubleVal}
	}
	return nil, nil
}
