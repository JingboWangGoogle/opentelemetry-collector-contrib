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
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configerror"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer"
)

const (
	// The value of "type" key in configuration.
	typeStr = "metrics_transform"
)

// Factory is the factory for metrics transform processor.
type Factory struct{}

// Type gets the type of the Option config created by this factory.
func (f *Factory) Type() configmodels.Type {
	return typeStr
}

// CreateDefaultConfig creates the default configuration for processor.
func (f *Factory) CreateDefaultConfig() configmodels.Processor {
	return &Config{
		ProcessorSettings: configmodels.ProcessorSettings{
			TypeVal: typeStr,
			NameVal: typeStr,
		},
	}
}

// CreateTraceProcessor creates a trace processor based on this config.
func (f *Factory) CreateTraceProcessor(
	ctx context.Context,
	params component.ProcessorCreateParams,
	nextConsumer consumer.TraceConsumer,
	cfg configmodels.Processor,
) (component.TraceProcessor, error) {
	return nil, configerror.ErrDataTypeIsNotSupported
}

// CreateMetricsProcessor creates a metrics processor based on this config.
func (f *Factory) CreateMetricsProcessor(
	ctx context.Context,
	params component.ProcessorCreateParams,
	nextConsumer consumer.MetricsConsumer,
	c configmodels.Processor,
) (component.MetricsProcessor, error) {
	oCfg := c.(*Config)
	err := validateConfiguration(*oCfg)
	if err == nil {
		return newMetricsTransformProcessor(nextConsumer, oCfg)
	}
	return nil, err
}

// buildConfiguration validates the input configuration has all of the required fields for the processor
// and returns a list of valid actions to configure the processor.
// An error is returned if there are any invalid inputs.
func validateConfiguration(config Config) error {
	if config.MetricName == "" {
		return fmt.Errorf("error creating \"metrics_transform\" processor due to missing required field \"metric_name\"")
	}

	if config.Action != Update && config.Action != Insert {
		return fmt.Errorf("error creating \"metrics_transform\" processor due to unsupported config \"action\": %v, the supported actions are \"insert\" and \"update\"", config.Action)
	}

	if config.Action == Insert && config.NewName == "" {
		return fmt.Errorf("error creating \"metrics_transform\" processor due to missing required field \"new_name\" while \"action\" is insert")
	}

	for i, op := range config.Operations {
		if op.Action != UpdateLabel && op.Action != AggregateLabels && op.Action != AggregateLabelValues {
			return fmt.Errorf("error creating \"metrics_transform\" processor due to unsupported operation \"action\": %v, the supported actions are \"update_label\", \"aggregate_labels\", and \"aggregate_label_values\"", op.Action)
		}

		if op.Action == UpdateLabel && op.Label == "" {
			return fmt.Errorf("error creating \"metrics_transform\" processor due to missing required field \"label\" while \"action\" is update_label in the %vth operation", i)
		}
	}

	return nil
}
