package summarizer

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/caio/go-tdigest/v4"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"

	prompb "github.com/prometheus/prometheus/prompb"
	"github.com/gogo/protobuf/proto"

	coll "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	met "go.opentelemetry.io/proto/otlp/metrics/v1"
	resv1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

type processor struct {
	windowSec int
	svcAttr   string
	state     map[string]*svc
	last      map[string]float64
}

type svc struct {
	td    *tdigest.TDigest
	req   float64
	ok    float64
	err   float64
	count uint64
}

func New(cfg config.ProcessorCfg) *processor {

	w := cfg.WindowSeconds
	if w <= 0 {
		w = 60
	}

	return &processor{
		windowSec: w,
		svcAttr:   cfg.ExtraString("service_attribute", "service.name"),
		state:     map[string]*svc{},
		last:      map[string]float64{},
	}
}

func (p *processor) Start(ctx context.Context, in <-chan any, out chan<- any) error {

	defer close(out)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	winStart := trunc(time.Now().Unix(), int64(p.windowSec))

	for {

		select {

		case <-ctx.Done():
			return nil

		case v, ok := <-in:
			if !ok {
				return nil
			}

			env, ok := v.(model.Envelope)
			if !ok {
				out <- v
				continue
			}

			switch env.Kind {

			case model.KindMetrics:
				p.consumeOTLP(env.Bytes)

			case model.KindPromRW:
				p.consumePromRW(env.Bytes)

			default:
				out <- v
			}

		case now := <-ticker.C:

			if now.Unix() >= winStart+int64(p.windowSec) {
				p.flush(out, winStart)
				winStart = trunc(now.Unix(), int64(p.windowSec))
			}
		}
	}
}

func (p *processor) flush(out chan<- any, winStart int64) {

	winEnd := winStart + int64(p.windowSec)

	for svcName, st := range p.state {

		var p50, p95, p99 float64

		if st.td != nil && st.td.Count() > 0 {
			p50 = st.td.Quantile(0.5)
			p95 = st.td.Quantile(0.95)
			p99 = st.td.Quantile(0.99)
		}

		rps := st.req / float64(p.windowSec)

		errRate := 0.0
		if total := st.ok + st.err; total > 0 {
			errRate = st.err / total
		}

		out <- model.Aggregate{
			Service:     svcName,
			WindowStart: winStart,
			WindowEnd:   winEnd,
			Count:       st.count,
			P50:         p50,
			P95:         p95,
			P99:         p99,
			RPS:         rps,
			ErrorRate:   errRate,
		}
	}

	p.state = map[string]*svc{}
}

func (p *processor) consumeOTLP(raw []byte) {

	var em coll.ExportMetricsServiceRequest

	if err := proto.Unmarshal(raw, &em); err != nil {
		log.Println("OTLP decode error:", err)
		return
	}

	for _, rm := range em.ResourceMetrics {

		svc := attrsToMap(rm.GetResource())[p.svcAttr]
		st := p.ensureSvc(svc)

		for _, sm := range rm.ScopeMetrics {

			for _, m := range sm.Metrics {

				switch d := m.Data.(type) {

				case *met.Metric_Histogram:
					p.consumeHistogram(d.Histogram, st)

				case *met.Metric_Sum:
					p.consumeSum(m.Name, d.Sum, st)
				}
			}
		}
	}
}

func (p *processor) consumeHistogram(h *met.Histogram, st *svc) {

	for _, dp := range h.DataPoints {

		for i, c := range dp.BucketCounts {

			if i >= len(dp.ExplicitBounds) {
				continue
			}

			ub := dp.ExplicitBounds[i]

			for j := 0; j < int(c); j++ {
				st.td.Add(ub)
			}

			st.count += uint64(c)
		}
	}
}

func (p *processor) consumeSum(name string, s *met.Sum, st *svc) {

	for _, dp := range s.DataPoints {

		val := numberOf(dp)

		if strings.HasSuffix(name, "requests_total") {
			st.req += val
			st.ok += val
			st.count += uint64(val)
		}

		if strings.HasSuffix(name, "errors_total") {
			st.err += val
			st.req += val
			st.count += uint64(val)
		}
	}
}

func (p *processor) consumePromRW(raw []byte) {

	var wr prompb.WriteRequest

	if err := proto.Unmarshal(raw, &wr); err != nil {
		log.Println("PromRW decode error:", err)
		return
	}

	for _, ts := range wr.Timeseries {

		lbls := labelsToMap(ts.Labels)

		svc := firstNonEmpty(lbls[p.svcAttr], lbls["job"], "unknown")

		st := p.ensureSvc(svc)

		name := lbls["__name__"]

		if strings.HasSuffix(name, "_bucket") {

			le := lbls["le"]
			if le == "" {
				continue
			}

			for _, s := range ts.Samples {
				st.td.Add(s.Value)
				st.count++
			}
		}
	}
}

func (p *processor) ensureSvc(name string) *svc {

	if s, ok := p.state[name]; ok {
		return s
	}

	td, _ := tdigest.New()

	ns := &svc{
		td: td,
	}

	p.state[name] = ns
	return ns
}

func trunc(ts int64, win int64) int64 {
	return ts - (ts % win)
}

func attrsToMap(r *resv1.Resource) map[string]string {

	out := map[string]string{}

	if r == nil {
		return out
	}

	for _, a := range r.Attributes {
		out[a.Key] = a.Value.GetStringValue()
	}

	return out
}

func labelsToMap(lbls []prompb.Label) map[string]string {

	m := map[string]string{}

	for _, l := range lbls {
		m[l.Name] = l.Value
	}

	return m
}

func numberOf(dp *met.NumberDataPoint) float64 {

	switch v := dp.Value.(type) {

	case *met.NumberDataPoint_AsInt:
		return float64(v.AsInt)

	case *met.NumberDataPoint_AsDouble:
		return v.AsDouble
	}

	return 0
}

func firstNonEmpty(ss ...string) string {

	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}
