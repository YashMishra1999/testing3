package vectorizer

import (
	"bytes"
	"context"
	"encoding/json"
	"hash/fnv"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

type processor struct {
	mode          string
	allowFallback bool

	ollamaEndpoint string
	ollamaModel    string
	client         *http.Client
	retries        int

	hashDim    int
	hashNgrams int
	lowercase  bool
	stopwords  map[string]struct{}
	tokenSplit *regexp.Regexp

	logsInclude []string
	ema         map[string][]float64
	traceMax    int
}

type pcaModel struct {
	rows int
	cols int
	mat  []float64
	mean []float64
	std  []float64
}

func New(cfg config.ProcessorCfg) *processor {

	mode := strings.ToLower(cfg.ExtraString("mode", "ollama"))
	endpoint := strings.TrimSuffix(cfg.ExtraString("endpoint", "http://localhost:11434"), "/")
	modelName := cfg.ExtraString("model", "nomic-embed-text")

	timeoutMs := 5000
	if v, ok := cfg.Extra["timeout_ms"].(int); ok && v > 0 {
		timeoutMs = v
	}

	retries := 2
	if v, ok := cfg.Extra["retries"].(int); ok {
		retries = v
	}

	allowFallback := true
	if v, ok := cfg.Extra["allow_fallback"].(bool); ok {
		allowFallback = v
	}

	hashDim := 384
	if v, ok := cfg.Extra["hash_dim"].(int); ok && v > 0 {
		hashDim = v
	}

	hashN := 2
	if v, ok := cfg.Extra["hash_ngrams"].(int); ok && v >= 1 && v <= 3 {
		hashN = v
	}

	lower := true
	if v, ok := cfg.Extra["lowercase"].(bool); ok {
		lower = v
	}

	stop := map[string]struct{}{}

	var logsInc []string
	if arr, ok := nestedStringSlice(cfg.Extra, "logs", "include_fields"); ok {
		logsInc = arr
	}

	return &processor{
		mode:           mode,
		allowFallback:  allowFallback,
		ollamaEndpoint: endpoint,
		ollamaModel:    modelName,
		client:         &http.Client{Timeout: time.Duration(timeoutMs) * time.Millisecond},
		retries:        retries,
		hashDim:        hashDim,
		hashNgrams:     hashN,
		lowercase:      lower,
		stopwords:      stop,
		tokenSplit:     regexp.MustCompile(`[^a-zA-Z0-9]+`),
		logsInclude:    logsInc,
		ema:            map[string][]float64{},
		traceMax:       6,
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

			a, ok := v.(model.Aggregate)
			if !ok {
				out <- v
				continue
			}

			source := strings.ToLower(a.Labels["source"])

			switch source {
			case "logs":
				a.Vector = p.embedText(ctx, p.buildLogsText(a))
			case "traces":
				a.Vector = p.embedText(ctx, p.buildTracesText(a))
			case "metrics":
				a.Vector = p.buildMetricsVector(a)
			default:
				a.Vector = p.embedText(ctx, a.SummaryText)
			}

			out <- a
		}
	}
}

func (p *processor) buildLogsText(a model.Aggregate) string {

	success := "true"
	if a.ErrorRate > 0 {
		success = "false"
	}

	parts := []string{
		"kind=log",
		"service=" + a.Service,
		"success=" + success,
	}

	if a.SummaryText != "" {
		parts = append(parts, a.SummaryText)
	}

	return strings.Join(parts, " ")
}

func (p *processor) buildMetricsVector(a model.Aggregate) []float32 {

	base := []float64{
		a.P50,
		a.P95,
		a.P99,
		a.RPS,
		a.ErrorRate,
	}

	out := make([]float32, len(base))

	for i := range base {
		out[i] = float32(base[i])
	}

	normalize(out)
	return out
}

func (p *processor) buildTracesText(a model.Aggregate) string {

	return strings.Join([]string{
		"kind=trace",
		"service=" + a.Service,
		"rps=" + formatFloat(a.RPS),
	}, " ")
}

func (p *processor) embedText(ctx context.Context, text string) []float32 {

	switch p.mode {

	case "ollama":
		emb, err := p.embedOllama(ctx, text)
		if err == nil {
			return emb
		}
		if p.allowFallback {
			return p.embedHashing(text)
		}

	case "hash":
		return p.embedHashing(text)
	}

	return nil
}

func (p *processor) embedOllama(ctx context.Context, text string) ([]float32, error) {

	body := map[string]any{
		"model":  p.ollamaModel,
		"prompt": text,
	}

	b, _ := json.Marshal(body)

	req, _ := http.NewRequestWithContext(ctx, "POST",
		p.ollamaEndpoint+"/api/embeddings",
		bytes.NewReader(b))

	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out struct {
		Embedding []float32 `json:"embedding"`
	}

	err = json.NewDecoder(resp.Body).Decode(&out)
	if err != nil {
		return nil, err
	}

	normalize(out.Embedding)
	return out.Embedding, nil
}

func (p *processor) embedHashing(text string) []float32 {

	toks := p.tokenize(text)
	vec := make([]float32, p.hashDim)

	for _, t := range toks {

		h := fnv.New64a()
		h.Write([]byte(t))
		idx := int(h.Sum64() % uint64(p.hashDim))

		vec[idx]++
	}

	normalize(vec)
	return vec
}

func (p *processor) tokenize(s string) []string {

	if p.lowercase {
		s = strings.ToLower(s)
	}

	return p.tokenSplit.Split(s, -1)
}

func normalize(v []float32) {

	var sum float64
	for _, x := range v {
		sum += float64(x * x)
	}

	if sum == 0 {
		return
	}

	inv := float32(1.0 / math.Sqrt(sum))

	for i := range v {
		v[i] *= inv
	}
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', 6, 64)
}

func nestedStringSlice(extra map[string]any, k1, k2 string) ([]string, bool) {

	n1, ok := extra[k1].(map[string]any)
	if !ok {
		return nil, false
	}

	raw, ok := n1[k2].([]any)
	if !ok {
		return nil, false
	}

	out := []string{}

	for _, it := range raw {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}

	return out, true
}
