package clients

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	elastic "github.com/elastic/go-elasticsearch/v8"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
)

type ElasticSearchEnvConfig struct {
	Username              string        `envconfig:"ELASTICSEARCH_USERNAME" required:"true"`
	Password              string        `envconfig:"ELASTICSEARCH_PASSWORD" required:"true"`
	Address               string        `envconfig:"ELASTICSEARCH_ADDRESS" required:"true"`
	Index                 string        `envconfig:"ELASTICSEARCH_INDEX" required:"true"`
	NumWorkers            int           `envconfig:"ELASTICSEARCH_BULK_WORKERS" default:"1"`
	FlushBytes            int           `envconfig:"ELASTICSEARCH_BULK_FLUSH_BYTES" default:"10000000"`
	FlushInterval         time.Duration `envconfig:"ELASTICSEARCH_BULK_FLUSH_INTERVAL" default:"30s"`
	BulkTimeout           time.Duration `envconfig:"ELASTICSEARCH_BULK_TIMEOUT" default:"90s"`
	ResponseTimeout       time.Duration `envconfig:"ELASTICSEARCH_RESPONSE_TIMEOUT" default:"90s"`
	DialTimeout           time.Duration `envconfig:"ELASTICSEARCH_DIAL_TIMEOUT" default:"1s"`
	SSLInsecureSkipVerify bool          `envconfig:"ELASTICSEARCH_SSL_INSECURE_SKIP_VERIFY" default:"true"`
	DocIdPrefix           string        `envconfig:"ELASTICSEARCH_CONFIG_DOC_ID_PREFIX" default:"inventory"`
}

func GetElasticConfigFromEnv() (ElasticSearchEnvConfig, error) {
	envConfig, err := getConfigFromEnv[ElasticSearchEnvConfig]()
	if err != nil {
		return ElasticSearchEnvConfig{}, fmt.Errorf("failed to parse elasticsearch config %w", err)
	}
	return envConfig, nil
}

func NewElasticsearchClient(config ElasticSearchEnvConfig) (*elastic.Client, error) {
	addresses := []string{
		config.Address,
	}
	cfg := elastic.Config{
		Addresses: addresses,
		Username:  config.Username,
		Password:  config.Password,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: config.ResponseTimeout,
			DialContext:           (&net.Dialer{Timeout: config.DialTimeout}).DialContext,
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
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	zap.S().Infof("connected to elastic search: %s", string(data))

	return client, nil
}

func getConfigFromEnv[T any]() (T, error) {
	var envConfig T
	err := envconfig.Process("", &envConfig)
	if err != nil {
		return envConfig, err
	}
	return envConfig, nil
}
