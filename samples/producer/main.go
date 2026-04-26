package main

import (
	"context"
	"encoding/json"
	"flag"
	"math/rand"
	"os"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/kubev2v/migration-event-streamer/internal/logger"
	pkgKafka "github.com/kubev2v/migration-event-streamer/pkg/kafka"
	"go.uber.org/zap"
)

const (
	inventoryEventType = "assisted.migrations.events.inventory"
	uiEventType        = "assisted.migrations.events.ui"
	agentEventType     = "assisted.migrations.events.agent"
	inputTopic         = "assisted.migrations.events"
	eventSource        = "com.redhat.assisted-migration"
)

var (
	inventory string
	sourceID  string
	timeout   string
	oneShot   bool
)

func main() {
	logger := logger.SetupLogger("console", "debug")
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	flag.StringVar(&inventory, "inventory", "", "")
	flag.StringVar(&sourceID, "source_id", uuid.NewString(), "")
	flag.StringVar(&timeout, "timetout", "1s", "")
	flag.BoolVar(&oneShot, "oneshot", false, "send only one message")
	flag.Parse()

	tick, err := time.ParseDuration(timeout)
	if err != nil {
		zap.S().Fatal(err)
	}

	data, err := os.ReadFile(inventory)
	if err != nil {
		zap.S().Fatal(err)
	}
	inv := make(map[string]interface{})
	if err := json.Unmarshal(data, &inv); err != nil {
		zap.S().Fatal(err)
	}

	producer, err := pkgKafka.NewKafkaProducer([]string{"127.0.0.1:9092"})
	if err != nil {
		zap.S().Fatalf("failed to create producer: %s", err)
	}
	defer producer.Close(context.Background())

	for {
		who := rand.Intn(3) + 1
		e := cloudevents.NewEvent()
		e.SetID(uuid.New().String())
		e.SetSource(eventSource)
		switch who {
		case 1:
			e.SetType(inventoryEventType)
			e.SetExtension("sourceID", sourceID)
			_ = e.SetData(cloudevents.ApplicationJSON, inv)
		case 2:
			e.SetType(uiEventType)
			_ = e.SetData(cloudevents.ApplicationJSON, map[string]string{"event": "click something"})
		case 3:
			e.SetType(agentEventType)
			_ = e.SetData(cloudevents.ApplicationJSON, map[string]string{"agent_state": "agent state"})
		}

		if err := producer.Write(context.Background(), inputTopic, e); err != nil {
			zap.S().Infof("failed to send: %v", err)
		} else {
			zap.S().Infof("sent, type %s", e.Context.GetType())
		}

		if oneShot {
			break
		}

		<-time.After(tick)
	}
}
