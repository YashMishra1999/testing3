package otlpgrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"time"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"

	colllog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collmet "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	colltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
)

type Receiver struct {
	endpoint   string
	tlsEnabled bool

	tlsCertFile     string
	tlsKeyFile      string
	tlsClientCAFile string
}

func New(rc config.ReceiverCfg) *Receiver {

	tlsEnabled, _ := nestedBool(rc.Extra, "tls", "enabled")

	return &Receiver{
		endpoint:        rc.Endpoint,
		tlsEnabled:      tlsEnabled,
		tlsCertFile:     nestedString(rc.Extra, "tls", "cert_file"),
		tlsKeyFile:      nestedString(rc.Extra, "tls", "key_file"),
		tlsClientCAFile: nestedString(rc.Extra, "tls", "client_ca_file"),
	}
}

func (r *Receiver) Start(ctx context.Context, out chan<- model.Envelope) error {

	addr := r.endpoint
	if addr == "" {
		addr = ":4317"
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	opts := []grpc.ServerOption{}

	if r.tlsEnabled {
		creds, err := r.buildTLS()
		if err != nil {
			return err
		}
		opts = append(opts, grpc.Creds(creds))
	}

	srv := grpc.NewServer(opts...)

	collmet.RegisterMetricsServiceServer(srv, &metricsSvc{out: out})
	colltrace.RegisterTraceServiceServer(srv, &tracesSvc{out: out})
	colllog.RegisterLogsServiceServer(srv, &logsSvc{out: out})

	reflection.Register(srv)

	go srv.Serve(lis)

	<-ctx.Done()
	srv.Stop()
	return nil
}

func (r *Receiver) buildTLS() (credentials.TransportCredentials, error) {

	if r.tlsCertFile == "" || r.tlsKeyFile == "" {
		return nil, errors.New("tls enabled but cert/key missing")
	}

	cert, err := tls.LoadX509KeyPair(r.tlsCertFile, r.tlsKeyFile)
	if err != nil {
		return nil, err
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	if r.tlsClientCAFile != "" {
		ca, err := os.ReadFile(r.tlsClientCAFile)
		if err != nil {
			return nil, err
		}

		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return credentials.NewTLS(cfg), nil
}

/* ================= SERVICES ================= */

type metricsSvc struct {
	collmet.UnimplementedMetricsServiceServer
	out chan<- model.Envelope
}

func (s *metricsSvc) Export(ctx context.Context,
	req *collmet.ExportMetricsServiceRequest,
) (*collmet.ExportMetricsServiceResponse, error) {

	b, _ := proto.Marshal(req)

	s.out <- model.Envelope{
		Kind:   model.KindMetrics,
		Bytes:  b,
		Attrs:  map[string]string{},
		TSUnix: time.Now().Unix(),
	}

	return &collmet.ExportMetricsServiceResponse{}, nil
}

/* -------- TRACES -------- */

type tracesSvc struct {
	colltrace.UnimplementedTraceServiceServer
	out chan<- model.Envelope
}

func (s *tracesSvc) Export(ctx context.Context,
	req *colltrace.ExportTraceServiceRequest,
) (*colltrace.ExportTraceServiceResponse, error) {

	b, _ := proto.Marshal(req)

	s.out <- model.Envelope{
		Kind:   model.KindTraces,
		Bytes:  b,
		Attrs:  map[string]string{},
		TSUnix: time.Now().Unix(),
	}

	return &colltrace.ExportTraceServiceResponse{}, nil
}

/* -------- LOGS -------- */

type logsSvc struct {
	colllog.UnimplementedLogsServiceServer
	out chan<- model.Envelope
}

func (s *logsSvc) Export(ctx context.Context,
	req *colllog.ExportLogsServiceRequest,
) (*colllog.ExportLogsServiceResponse, error) {

	b, _ := proto.Marshal(req)

	s.out <- model.Envelope{
		Kind:   model.KindJSONLogs,
		Bytes:  b,
		Attrs:  map[string]string{},
		TSUnix: time.Now().Unix(),
	}

	return &colllog.ExportLogsServiceResponse{}, nil
}

/* ================= HELPERS ================= */

func nestedString(m map[string]any, k1, k2 string) string {
	if n, ok := m[k1].(map[string]any); ok {
		if s, ok := n[k2].(string); ok {
			return s
		}
	}
	return ""
}

func nestedBool(m map[string]any, k1, k2 string) (bool, bool) {
	if n, ok := m[k1].(map[string]any); ok {
		if b, ok := n[k2].(bool); ok {
			return b, true
		}
	}
	return false, false
}
