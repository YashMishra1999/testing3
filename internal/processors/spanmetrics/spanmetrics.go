package spanmetrics

import (
	"context"
	"log"
	"math"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"

	collmet "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	com "go.opentelemetry.io/proto/otlp/common/v1"
	met "go.opentelemetry.io/proto/otlp/metrics/v1"
	tr "go.opentelemetry.io/proto/otlp/trace/v1"

	"google.golang.org/protobuf/proto"
)

type processor struct {
	dimensions     []string
	histBounds     []float64
	defaultSvcAttr string
}

func New(cfg config.ProcessorCfg) *processor {

	// histogram buckets
	bounds := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

	if xs, ok := cfg.Extra["histogram_buckets"].([]any); ok {
		tmp := []float64{}
		for _, v := range xs {
			if f, ok := v.(float64); ok {
				tmp = append(tmp, f)
			}
		}
		if len(tmp) > 0 {
			bounds = tmp
		}
	}

	// dimensions
	dims := []string{}
	if xs, ok := cfg.Extra["dimensions"].([]any); ok {
		for _, v := range xs {
			if s, ok := v.(string); ok {
				dims = append(dims, s)
			}
		}
	}

	return &processor{
		dimensions:     dims,
		histBounds:     bounds,
		defaultSvcAttr: "service.name",
	}
}

func (p *processor) Start(ctx context.Context, in <-chan any, out chan<- any) error {

	defer close(out)

	for {
		select {

		case <-ctx.Done():
			return nil

		case v, ok := <-in:

			if !ok {
				return nil
			}

			env, ok := v.(model.Envelope)
			if !ok || env.Kind != model.KindTraces {
				out <- v
				continue
			}

			rm := p.tracesToResourceMetrics(env.Bytes)

			if len(rm) == 0 {
				out <- v
				continue
			}

			em := &collmet.ExportMetricsServiceRequest{
				ResourceMetrics: rm,
			}

			b, err := proto.Marshal(em)
			if err != nil {
				log.Println("marshal metrics error:", err)
				continue
			}

			out <- model.Envelope{
				Kind:   model.KindMetrics,
				Bytes:  b,
				Attrs:  env.Attrs,
				TSUnix: env.TSUnix,
			}
		}
	}
}

func (p *processor) tracesToResourceMetrics(raw []byte) []*met.ResourceMetrics {

	var et tr.TracesData

	if err := proto.Unmarshal(raw, &et); err != nil {
		log.Println("trace decode error:", err)
		return nil
	}

	var out []*met.ResourceMetrics

	for _, rt := range et.ResourceSpans {

		rm := &met.ResourceMetrics{
			Resource: rt.Resource,
		}

		sm := &met.ScopeMetrics{}
		rm.ScopeMetrics = append(rm.ScopeMetrics, sm)

		for _, ss := range rt.ScopeSpans {

			for _, sp := range ss.Spans {

				start := uint64(sp.StartTimeUnixNano)
				end := safeEnd(start, sp.EndTimeUnixNano)

				dur := float64(end-start) / 1e9

				lbl := []*com.KeyValue{
					strKV("service", "unknown"),
				}

				sm.Metrics = append(sm.Metrics,
					p.buildRequestsTotal(int64(start), int64(end), lbl),
				)

				sm.Metrics = append(sm.Metrics,
					p.buildDurationHistogram(int64(start), int64(end), dur, lbl),
				)
			}
		}

		out = append(out, rm)
	}

	return out
}

func (p *processor) buildRequestsTotal(tsStart, tsEnd int64, labels []*com.KeyValue) *met.Metric {

	dp := &met.NumberDataPoint{
		TimeUnixNano:      uint64(tsEnd),
		StartTimeUnixNano: uint64(tsStart),
		Attributes:        labels,
		Value:             &met.NumberDataPoint_AsDouble{AsDouble: 1},
	}

	return &met.Metric{
		Name: "requests_total",
		Data: &met.Metric_Sum{
			Sum: &met.Sum{
				IsMonotonic: true,
				DataPoints:  []*met.NumberDataPoint{dp},
			},
		},
	}
}

func (p *processor) buildDurationHistogram(tsStart, tsEnd int64, dur float64, labels []*com.KeyValue) *met.Metric {

	idx := bucketIndex(p.histBounds, dur)

	bCounts := make([]uint64, len(p.histBounds)+1)
	bCounts[idx] = 1

	dp := &met.HistogramDataPoint{
		TimeUnixNano:      uint64(tsEnd),
		StartTimeUnixNano: uint64(tsStart),
		Attributes:        labels,
		ExplicitBounds:    p.histBounds,
		BucketCounts:      bCounts,
		Count:             1,
		Sum:               protoFloat64(dur),
	}

	return &met.Metric{
		Name: "duration_seconds",
		Data: &met.Metric_Histogram{
			Histogram: &met.Histogram{
				DataPoints: []*met.HistogramDataPoint{dp},
			},
		},
	}
}

func strKV(k, v string) *com.KeyValue {
	return &com.KeyValue{
		Key: k,
		Value: &com.AnyValue{
			Value: &com.AnyValue_StringValue{StringValue: v},
		},
	}
}

func protoFloat64(v float64) *float64 {
	x := v
	return &x
}

func bucketIndex(bounds []float64, v float64) int {
	for i := range bounds {
		if v <= bounds[i] || math.IsNaN(v) {
			return i
		}
	}
	return len(bounds)
}

func safeEnd(start, end uint64) uint64 {
	if end == 0 || end < start {
		return start
	}
	return end
}
