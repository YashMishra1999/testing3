package promrw

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/golang/snappy"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

type Receiver struct {
	endpoint string
	path     string

	maxBodyBytes int64
	readTimeout  time.Duration
	writeTimeout time.Duration
	idleTimeout  time.Duration

	tlsEnabled        bool
	tlsCertFile       string
	tlsKeyFile        string
	tlsClientCAFile   string
	requireClientCert bool
}

func New(rc config.ReceiverCfg) *Receiver {

	path := "/api/v1/write"
	if s, ok := rc.Extra["path"].(string); ok && s != "" {
		path = s
	}

	maxBody := int64(16 * 1024 * 1024)
	if v, ok := rc.Extra["max_body_bytes"].(int); ok && v > 0 {
		maxBody = int64(v)
	}

	rt := 30 * time.Second
	if v, ok := rc.Extra["read_timeout_ms"].(int); ok {
		rt = time.Duration(v) * time.Millisecond
	}

	wt := 30 * time.Second
	if v, ok := rc.Extra["write_timeout_ms"].(int); ok {
		wt = time.Duration(v) * time.Millisecond
	}

	it := 120 * time.Second
	if v, ok := rc.Extra["idle_timeout_ms"].(int); ok {
		it = time.Duration(v) * time.Millisecond
	}

	tlsEnabled, _ := nestedBool(rc.Extra, "tls", "enabled")

	return &Receiver{
		endpoint:          rc.Endpoint,
		path:              path,
		maxBodyBytes:      maxBody,
		readTimeout:       rt,
		writeTimeout:      wt,
		idleTimeout:       it,
		tlsEnabled:        tlsEnabled,
		tlsCertFile:       nestedString(rc.Extra, "tls", "cert_file"),
		tlsKeyFile:        nestedString(rc.Extra, "tls", "key_file"),
		tlsClientCAFile:   nestedString(rc.Extra, "tls", "client_ca_file"),
		requireClientCert: nestedBoolDefault(rc.Extra, "tls", "require_client_cert"),
	}
}

func (r *Receiver) Start(ctx context.Context, out chan<- model.Envelope) error {

	addr := r.endpoint
	if addr == "" {
		addr = ":19291"
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc(r.path, func(w http.ResponseWriter, req *http.Request) {

		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		reader := http.MaxBytesReader(w, req.Body, r.maxBodyBytes)
		defer req.Body.Close()

		encoding := strings.ToLower(req.Header.Get("Content-Encoding"))

		if strings.Contains(encoding, "gzip") {
			gr, err := gzip.NewReader(reader)
			if err != nil {
				http.Error(w, "invalid gzip", 400)
				return
			}
			defer gr.Close()
			reader = gr
		}

		body, err := io.ReadAll(reader)
		if err != nil {
			http.Error(w, "read error", 400)
			return
		}

		if strings.Contains(encoding, "snappy") || looksSnappy(body) {
			body, err = snappy.Decode(nil, body)
			if err != nil {
				http.Error(w, "invalid snappy", 400)
				return
			}
		}

		env := model.Envelope{
			Kind:   model.KindPromRW,
			Bytes:  body,
			TSUnix: time.Now().Unix(),
		}

		select {
		case out <- env:
		default:
			log.Println("[promrw] dropping request (backpressure)")
		}

		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  r.readTimeout,
		WriteTimeout: r.writeTimeout,
		IdleTimeout:  r.idleTimeout,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	if r.tlsEnabled {
		tlsCfg, err := r.buildTLS()
		if err != nil {
			return err
		}
		srv.TLSConfig = tlsCfg
	}

	go func() {
		if r.tlsEnabled {
			srv.ServeTLS(ln, "", "")
		} else {
			srv.Serve(ln)
		}
	}()

	<-ctx.Done()
	return srv.Shutdown(context.Background())
}

func (r *Receiver) buildTLS() (*tls.Config, error) {

	if r.tlsCertFile == "" || r.tlsKeyFile == "" {
		return nil, errors.New("tls enabled but cert or key missing")
	}

	cert, err := tls.LoadX509KeyPair(r.tlsCertFile, r.tlsKeyFile)
	if err != nil {
		return nil, err
	}

	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}

	if r.tlsClientCAFile != "" {

		pem, err := os.ReadFile(r.tlsClientCAFile)
		if err != nil {
			return nil, err
		}

		cp := x509.NewCertPool()
		cp.AppendCertsFromPEM(pem)

		cfg.ClientCAs = cp

		if r.requireClientCert {
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}

	return cfg, nil
}

func looksSnappy(b []byte) bool {
	if len(b) < 10 {
		return false
	}
	_, err := snappy.Decode(nil, b[:min(len(b), 256)])
	return err == nil
}

func nestedString(m map[string]any, k1, k2 string) string {

	if n1, ok := m[k1].(map[string]any); ok {
		if s, ok := n1[k2].(string); ok {
			return s
		}
	}
	return ""
}

func nestedBool(m map[string]any, k1, k2 string) (bool, bool) {

	if n1, ok := m[k1].(map[string]any); ok {
		b, ok := n1[k2].(bool)
		return b, ok
	}
	return false, false
}

func nestedBoolDefault(m map[string]any, k1, k2 string) bool {

	if n1, ok := m[k1].(map[string]any); ok {
		if b, ok := n1[k2].(bool); ok {
			return b
		}
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
