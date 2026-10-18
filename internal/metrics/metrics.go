package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	prometheus.MustRegister(received, completed, duration, retries, inFlight)
}

const (
	EventsReceivedTotal            = "streamer_events_received_total"
	EventsCompletedTotal           = "streamer_events_completed_total"
	EventProcessingDurationSeconds = "streamer_event_processing_duration_seconds"
	RetriesTotal                   = "streamer_retries_total"
	EventsInFlight                 = "streamer_events_in_flight"
	LastSuccessTimestampSeconds    = "streamer_last_success_timestamp_seconds"

	KafkaConsumerNamespace = "kafka_consumer"
)

const (
	LabelStage     = "stage"
	LabelEventType = "event_type"
	LabelOutcome   = "outcome"
	LabelConsumer  = "consumer"
)

const (
	StageRouter     = "router"
	StageDispatcher = "dispatcher"
	StagePipeline   = "pipeline"

	Success     = "success"
	Failed      = "failed"
	Invalid     = "invalid"
	Unsupported = "unsupported"
	Canceled    = "canceled"
)

var (
	eventTypes = make(map[string]struct{})

	received = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: EventsReceivedTotal,
		Help: "Events accepted for processing by each stage, including invalid and unsupported events.",
	}, []string{LabelStage})
	completed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: EventsCompletedTotal,
		Help: "Logical events completed by stage and final outcome; retries are not separate events.",
	}, []string{LabelStage, LabelEventType, LabelOutcome})
	duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    EventProcessingDurationSeconds,
		Help:    "Time spent performing stage work, including pipeline decoding, retries, and retry delays.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
	}, []string{LabelStage, LabelEventType, LabelOutcome})
	retries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: RetriesTotal,
		Help: "Additional processing attempts actually started after an initial failure.",
	}, []string{LabelStage, LabelEventType})
	inFlight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: EventsInFlight,
		Help: "Events currently being processed by each stage, including retry delays.",
	}, []string{LabelStage})
)

// Begin records a logical event. Call the returned function once with its final
// outcome when that stage's work completes: handoff for dispatcher, destination
// write or failure handling for router and pipeline.
func Begin(stage, name string) func(outcome string) {
	start := time.Now()
	received.WithLabelValues(stage).Inc()
	inFlight.WithLabelValues(stage).Inc()

	return callback(name, stage, start)
}

func Retry(stage, name string) {
	retries.WithLabelValues(stage, eventType(name)).Inc()
}

// RegisterEventType admits a code-configured pipeline name as a metric label.
// Unrecognized values from incoming events are grouped under "unknown".
func RegisterEventType(name string) {
	eventTypes[name] = struct{}{}
}

func eventType(name string) string {
	if _, ok := eventTypes[name]; ok {
		return name
	}
	return "non-pipeline"
}

func callback(name, stage string, start time.Time) func(string) {
	return func(outcome string) {
		eType := eventType(name)

		completed.WithLabelValues(stage, eType, outcome).Inc()
		duration.WithLabelValues(stage, eType, outcome).Observe(time.Since(start).Seconds())
		inFlight.WithLabelValues(stage).Dec()
	}
}
