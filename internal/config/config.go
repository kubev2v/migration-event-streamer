package config

import (
	"crypto/tls"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"time"
)

//go:generate go run github.com/ecordell/optgen -output zz_generated.configuration.go . Configuration Kafka ElasticSearch

type Configuration struct {
	Kafka         Kafka         `debugmap:"visible"`
	ElasticSearch ElasticSearch `debugmap:"visible"`
	LogFormat     string        `debugmap:"visible" default:"console"`
	LogLevel      string        `debugmap:"visible" default:"info"`
	MetricsPort   int           `debugmap:"visible" default:"8080"`
}

type Kafka struct {
	Brokers      []string `debugmap:"visible"`
	ClientID     string   `debugmap:"visible"`
	TLS          bool     `debugmap:"visible" default:"false"`
	SASLEnabled  bool     `debugmap:"visible" default:"false"`
	SASLUsername string   `debugmap:"hidden"`
	SASLPassword string   `debugmap:"hidden"`
}

type ElasticSearch struct {
	Username              string   `debugmap:"visible"`
	Password              string   `debugmap:"hidden"`
	Host                  string   `debugmap:"visible" default:"http://localhost:9200"`
	Indexes               []string `debugmap:"visible"`
	SSLInsecureSkipVerify bool     `debugmap:"sensitive" default:"true"`
	ResponseTimeout       string   `debugmap:"visible" default:"90s"`
	DialTimeout           string   `debugmap:"visible" default:"1s"`
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

func (k Kafka) ConnKgoOpts() []kgo.Opt {
	var opts []kgo.Opt
	if k.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	if k.SASLEnabled {
		auth := scram.Auth{User: k.SASLUsername, Pass: k.SASLPassword}
		opts = append(opts, kgo.SASL(auth.AsSha512Mechanism()))
	}
	return opts
}
