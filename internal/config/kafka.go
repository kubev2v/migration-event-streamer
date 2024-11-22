package config

import (
	"github.com/IBM/sarama"
)

// KafkaConfig gathers all kafka connection information.
type KafkaConfig struct {
	Brokers  []string            `envconfig:"KAKFA_BROKERS"`
	Version  sarama.KafkaVersion `yaml:"-"`
	ClientID string

	SaramaConfig *sarama.Config
}
