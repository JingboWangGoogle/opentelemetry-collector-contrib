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
	"log"

	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/consumer/pdatautil"
)

const (
	validNewNameFuncName       = "validNewName()"
	validNewLabelFuncName      = "validNewLabel()"
	validNewLabelValueFuncName = "validNewLabelValue()"
)

type metricsTransformProcessor struct {
	cfg        *Config
	next       consumer.MetricsConsumer
	transforms []Transform
}

var _ component.MetricsProcessor = (*metricsTransformProcessor)(nil)

func newMetricsTransformProcessor(next consumer.MetricsConsumer, cfg *Config) (*metricsTransformProcessor, error) {
	return &metricsTransformProcessor{
		cfg:        cfg,
		next:       next,
		transforms: cfg.Transforms,
	}, nil
}

// GetCapabilities returns the Capabilities associated with the metrics transform processor.
func (mtp *metricsTransformProcessor) GetCapabilities() component.ProcessorCapabilities {
	return component.ProcessorCapabilities{MutatesConsumedData: true}
}

// Start is invoked during service startup.
func (*metricsTransformProcessor) Start(ctx context.Context, host component.Host) error {
	return nil
}

// Shutdown is invoked during service shutdown.
func (*metricsTransformProcessor) Shutdown(ctx context.Context) error {
	return nil
}

// ConsumeMetrics implements the MetricsProcessor interface.
func (mtp *metricsTransformProcessor) ConsumeMetrics(ctx context.Context, md pdata.Metrics) error {
	return mtp.next.ConsumeMetrics(ctx, mtp.transform(md))
}

// transform transforms the metrics based on the information specified in the config.
func (mtp *metricsTransformProcessor) transform(md pdata.Metrics) pdata.Metrics {
	mds := pdatautil.MetricsToMetricsData(md)

	for i, data := range mds {
		nameToMetricMapping := make(map[string]*metricspb.Metric)
		// O(len(data.Metrics))
		for _, metric := range data.Metrics {
			nameToMetricMapping[metric.MetricDescriptor.Name] = metric
		}

		for _, transform := range mtp.transforms {
			if !mtp.validNewName(transform, nameToMetricMapping) {
				log.Printf("error running %q processor due to collision %q: %v with existing metric names detected by the function %q", typeStr, NewNameFieldName, transform.NewName, validNewNameFuncName)
				continue
			}

			metric, ok := nameToMetricMapping[transform.MetricName]
			if !ok {
				continue
			}

			// mtp.action is already validated to only contain either update or insert
			if transform.Action == Update {
				mtp.update(metric, transform)
				if transform.NewName == "" {
					continue
				}
				// if name is updated, the map has to be updated
				nameToMetricMapping[transform.NewName] = nameToMetricMapping[transform.MetricName]
				delete(nameToMetricMapping, transform.MetricName)
			} else if transform.Action == Insert {
				var newMetric *metricspb.Metric
				mds[i].Metrics, newMetric = mtp.insert(metric, mds[i].Metrics, transform)
				// mapping has to be updated with the name metric
				nameToMetricMapping[newMetric.MetricDescriptor.Name] = newMetric
			}
		}
	}

	return pdatautil.MetricsFromMetricsData(mds)
}
