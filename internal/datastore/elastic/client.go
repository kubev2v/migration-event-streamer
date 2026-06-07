package elastic

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	elastic "github.com/elastic/go-elasticsearch/v8"
	"github.com/kubev2v/migration-event-streamer/internal/config"
	"go.uber.org/zap"
)

func NewElasticsearchClient(config config.ElasticSearch) (*elastic.Client, error) {
	host := config.Host
	if host != "" && !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		host = "https://" + host
	}
	addresses := []string{
		host,
	}
	cfg := elastic.Config{
		Addresses: addresses,
		Username:  config.Username,
		Password:  config.Password,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: config.GetResponseTimeout(),
			DialContext:           (&net.Dialer{Timeout: config.GetDialTimeout()}).DialContext,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: config.SSLInsecureSkipVerify,
				MinVersion:         tls.VersionTLS11,
			},
		},
	}

	client, err := elastic.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize elasticsearch client %w", err)
	}

	resp, err := client.Info()
	if err != nil {
		return nil, fmt.Errorf("failed to get info from elasticsearch server: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	data, _ := io.ReadAll(resp.Body)
	zap.S().Infof("connected to elastic search: %s", string(data))

	return client, nil
}
