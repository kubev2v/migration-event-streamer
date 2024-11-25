package main

import (
	"context"
	"encoding/json"
	"flag"
	"math/rand"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/kubev2v/migration-event-streamer/internal/logger"
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
	logger := logger.SetupLogger()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.MaxVersion

	sender, err := kafka_sarama.NewSender([]string{"127.0.0.1:9092"}, saramaConfig, inputTopic)
	if err != nil {
		zap.S().Fatalf("failed to create protocol %s", err)
	}

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

	defer sender.Close(context.Background())

	c, err := cloudevents.NewClient(sender, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		zap.S().Fatalf("failed to create client %s", err)
	}

	for {
		now := time.Now().Unix()

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

		if result := c.Send(
			// Set the producer message key
			kafka_sarama.WithMessageKey(context.Background(), sarama.StringEncoder(e.ID())),
			e,
		); cloudevents.IsUndelivered(result) {
			zap.S().Infof("failed to send: %v", err)
		} else {
			zap.S().Infof("sent: %v, accepted: %t, type %s", now, cloudevents.IsACK(result), e.Context.GetType())
		}

		if oneShot {
			break
		}

		<-time.After(tick)
	}
}
