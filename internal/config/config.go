package config

import "time"

//go:generate go run github.com/ecordell/optgen -output zz_generated.configuration.go . Configuration Kafka ElasticSearch

type Configuration struct {
	Kafka         Kafka         `debugmap:"visible"`
	ElasticSearch ElasticSearch `debugmap:"visible"`
	LogFormat     string        `debugmap:"visible" default:"console"`
	LogLevel      string        `debugmap:"visible" default:"info"`
	MetricsPort   int           `debugmap:"visible" default:"8080"`
}

type Kafka struct {
	Brokers  []string `debugmap:"visible"`
	ClientID string   `debugmap:"visible"`
}

type ElasticSearch struct {
	Username              string `debugmap:"visible"`
	Password              string `debugmap:"hidden"`
	Host                  string `debugmap:"visible" default:"http://localhost:9200"`
	IndexPrefix           string `debugmap:"visible" default:"assisted_migrations"`
	Indexes               []string `debugmap:"visible"`
	SSLInsecureSkipVerify bool   `debugmap:"sensitive" default:"true"`
	ResponseTimeout       string `debugmap:"visible" default:"90s"`
	DialTimeout           string `debugmap:"visible" default:"1s"`
}

func (e ElasticSearch) GetResponseTimeout() time.Duration {
	d, err := time.ParseDuration(e.ResponseTimeout)
	if err != nil {
		return 30 * time.Second
	}
	return d
}

func (e ElasticSearch) GetDialTimeout() time.Duration {
	d, err := time.ParseDuration(e.DialTimeout)
	if err != nil {
		return 1 * time.Second
	}
	return d
}
