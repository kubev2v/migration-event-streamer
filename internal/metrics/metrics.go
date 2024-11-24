package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	subSystem                   = "assisted_migrations"
	labelEventType              = "topic"
	totalMessagesCount          = "messages_total_count"
	totalProcessedMessagesCount = "messages_processed_total_count"
	totalProcessingErrorsCount  = "processing_errors_total_count"
	processingDuration          = "processing_duration_in_ms"
)

var (
	labels = []string{labelEventType}

	totalMessagesCountMetrics = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subSystem,
			Name:      totalMessagesCount,
			Help:      "number of messages received",
		},
		labels,
	)

	totalProcessedMessagesCountMetrics = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subSystem,
			Name:      totalProcessedMessagesCount,
			Help:      "number of processed messages",
		},
		labels,
	)

	totalProcessingErrorsCountMetrics = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subSystem,
			Name:      totalProcessingErrorsCount,
			Help:      "number of processing errors",
		},
		labels,
	)

	processingDurationMetrics = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: subSystem,
			Name:      processingDuration,
			Help:      `processing duration in milliseconds`,
			Buckets: []float64{
				10.0,
				30.0,
				60.0,
				120.0,
				150.0,
				180.0,
				210.0,
				240.0,
				270.0,
				300.0,
				330.0,
				360.0,
				390.0,
				420.0,
				450.0,
				480.0,
				510.0,
				540.0,
				570.0,
				600.0,
				900.0,
				1200.0,
				1800.0,
				2400.0,
				3600.0,
				4800.0,
				7200.0,
			},
		},
		labels,
	)
)

func IncreaseMessagesCount(eventType string) {
	labels := prometheus.Labels{
		labelEventType: eventType,
	}
	totalMessagesCountMetrics.With(labels).Inc()
}

func IncreaseProcessedMessagesCount(eventType string) {
	labels := prometheus.Labels{
		labelEventType: eventType,
	}
	totalProcessedMessagesCountMetrics.With(labels).Inc()
}

func IncreaseErrorProcessingCount(eventType string) {
	labels := prometheus.Labels{
		labelEventType: eventType,
	}
	totalProcessingErrorsCountMetrics.With(labels).Inc()
}

func UpdateProcessingMetric(eventType string, elapsed time.Duration) {
	labels := prometheus.Labels{
		labelEventType: eventType,
	}
	processingDurationMetrics.With(labels).Observe(float64(elapsed.Milliseconds()))
}

func init() {
	registerMetrics()
}

func registerMetrics() {
	prometheus.MustRegister(totalMessagesCountMetrics)
	prometheus.MustRegister(totalProcessingErrorsCountMetrics)
	prometheus.MustRegister(totalProcessedMessagesCountMetrics)
	prometheus.MustRegister(processingDurationMetrics)
}
