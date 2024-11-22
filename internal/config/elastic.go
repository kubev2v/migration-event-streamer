package config

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
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

func getConfigFromEnv[T any]() (T, error) {
	var envConfig T
	err := envconfig.Process("", &envConfig)
	if err != nil {
		return envConfig, err
	}
	return envConfig, nil
}
