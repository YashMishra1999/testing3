package pulsar

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	ps "github.com/apache/pulsar-client-go/pulsar"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

type Receiver struct {
	serviceURL string
	topic      string
	subName    string
	kind       string

	subType           ps.SubscriptionType
	authToken         string
	authTokenFile     string
	tlsAllowInsecure  bool
	tlsTrustCertsPath string
	receiverQueueSize int
}

func New(rc config.ReceiverCfg, kind string) *Receiver {

	svc := strings.TrimSpace(rc.Endpoint)
	if svc == "" && len(rc.Brokers) > 0 {
		svc = rc.Brokers[0]
	}

	subType := ps.Shared
	if s, ok := rc.Extra["subscription_type"].(string); ok {
		switch strings.ToLower(s) {
		case "exclusive":
			subType = ps.Exclusive
		case "failover":
			subType = ps.Failover
		case "key_shared":
			subType = ps.KeyShared
		}
	}

	authToken, _ := rc.Extra["auth_token"].(string)
	authTokenFile, _ := rc.Extra["auth_token_file"].(string)
	tlsAllowInsecure, _ := rc.Extra["tls_allow_insecure"].(bool)
	tlsTrustPath, _ := rc.Extra["tls_trust_certs_file"].(string)

	queue := 1000
	if v, ok := rc.Extra["receiver_queue_size"].(int); ok && v > 0 {
		queue = v
	}

	return &Receiver{
		serviceURL:        svc,
		topic:             rc.Topic,
		subName:           rc.Group,
		kind:              normalizeKind(kind),
		subType:           subType,
		authToken:         authToken,
		authTokenFile:     authTokenFile,
		tlsAllowInsecure:  tlsAllowInsecure,
		tlsTrustCertsPath: tlsTrustPath,
		receiverQueueSize: queue,
	}
}

func (r *Receiver) Start(ctx context.Context, out chan<- model.Envelope) error {

	if r.serviceURL == "" || r.topic == "" || r.subName == "" {
		return errors.New("pulsar missing config")
	}

	opts := ps.ClientOptions{
		URL:                        r.serviceURL,
		TLSAllowInsecureConnection: r.tlsAllowInsecure,
		TLSTrustCertsFilePath:      r.tlsTrustCertsPath,
	}

	if r.authToken != "" {
		opts.Authentication = ps.NewAuthenticationToken(r.authToken)
	} else if r.authTokenFile != "" {
		opts.Authentication = ps.NewAuthenticationTokenFromFile(r.authTokenFile)
	}

	client, err := ps.NewClient(opts)
	if err != nil {
		return err
	}
	defer client.Close()

	consumer, err := client.Subscribe(ps.ConsumerOptions{
		Topic:             r.topic,
		SubscriptionName:  r.subName,
		Type:              r.subType,
		ReceiverQueueSize: r.receiverQueueSize,
	})
	if err != nil {
		return err
	}
	defer consumer.Close()

	log.Printf("[pulsar] consuming topic=%s", r.topic)

	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			return err
		}

		env := model.Envelope{
			Kind:   mapKind(r.kind),
			Bytes:  msg.Payload(),
			Attrs:  msg.Properties(),
			TSUnix: time.Now().Unix(),
		}

		select {
		case out <- env:
		default:
			log.Printf("[pulsar] dropping message")
		}

		consumer.Ack(msg)
	}
}

func mapKind(k string) string {
	switch k {
	case "metrics":
		return model.KindMetrics
	case "traces":
		return model.KindTraces
	case "prom_rw":
		return model.KindPromRW
	case "json_logs":
		return model.KindJSONLogs
	default:
		return model.KindMetrics
	}
}

func normalizeKind(k string) string {
	k = strings.ToLower(strings.TrimSpace(k))
	switch k {
	case "metrics", "traces", "prom_rw", "json_logs":
		return k
	default:
		return "metrics"
	}
}
