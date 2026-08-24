package elastic

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/kubev2v/migration-event-streamer/internal/config"
	opensearch "github.com/opensearch-project/opensearch-go/v4"
	opensearchapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"go.uber.org/zap"
)

func NewElasticsearchClient(config config.ElasticSearch) (*opensearchapi.Client, error) {
	host := config.Host
	if host != "" && !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		host = "https://" + host
	}
	addresses := []string{
		host,
	}

	// Clone DefaultTransport to preserve connection pooling, HTTP/2, and timeouts
	tp := http.DefaultTransport.(*http.Transport).Clone()
	tp.MaxIdleConnsPerHost = 10
	tp.ResponseHeaderTimeout = config.GetResponseTimeout()
	tp.DialContext = (&net.Dialer{
		Timeout: config.GetDialTimeout(),
	}).DialContext
	tp.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: config.SSLInsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}

	cfg := opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: addresses,
			Username:  config.Username,
			Password:  config.Password,
			Transport: tp,
		},
	}

	client, err := opensearchapi.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize opensearch client %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Info(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get info from opensearch server: %w", err)
	}

	zap.S().Infof("connected to opensearch: version=%s, cluster=%s", resp.Version.Number, resp.ClusterName)

	return client, nil
}
