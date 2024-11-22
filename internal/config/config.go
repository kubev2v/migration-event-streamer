package config

import (
	"time"

	"github.com/IBM/sarama"
)

type Route struct {
	EventType string `yaml:"eventType"`
	Topic     string `yaml:"topic"`
}

type Router struct {
	InputTopic string  `yaml:"inputTopic"`
	Routes     []Route `yaml:"routes"`
}

type Pipeline struct {
	Name        string `yaml:"name"`
	Type        string `yaml:"type"`
	InputTopic  string `yaml:"inputTopic"`
	OutputTopic string `yaml:"outputTopic"`
}

// KafkaConfig gathers all kafka connection information.
type KafkaConfig struct {
	Brokers  []string            `yaml:"brokers"`
	Version  sarama.KafkaVersion `yaml:"-"`
	ClientID string              `yaml:"clientID"`

	SaramaConfig *sarama.Config
}

type ElasticSearchConfig struct {
	Username              string   `yaml:"username"`
	Password              string   `yaml:"password"`
	Host                  string   `yaml:"host"`
	IndexPrefix           string   `yaml:"indexPrefix"`
	Indexes               []string `yaml:"indexes"`
	SSLInsecureSkipVerify bool     `yaml:"sslInsecureSkipVerify"`
	ResponseTimeout       string   `yaml:"responseTimeout"`
	DialTimeout           string   `yaml:"dialTimeout"`
}

func (e ElasticSearchConfig) GetResponseTimeout() time.Duration {
	d, err := time.ParseDuration(e.ResponseTimeout)
	if err != nil {
		return time.Duration(30 * time.Second)
	}
	return d
}

func (e ElasticSearchConfig) GetDialTimeout() time.Duration {
	d, err := time.ParseDuration(e.ResponseTimeout)
	if err != nil {
		return time.Duration(1 * time.Second)
	}
	return d
}

type StreamerConfig struct {
	Kafka     KafkaConfig         `yaml:"kafka"`
	Elastic   ElasticSearchConfig `yaml:"elastic"`
	Router    Router              `yaml:"router"`
	Pipelines []Pipeline          `yaml:"pipelines"`
}
