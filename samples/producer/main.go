package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/kubev2v/migration-event-streamer/internal/logger"
	"go.uber.org/zap"
)

var (
	inventory string
	sourceID  string
)

func main() {
	logger := logger.SetupLogger()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.MaxVersion

	sender, err := kafka_sarama.NewSender([]string{"127.0.0.1:9092"}, saramaConfig, "assisted.migrations.events")
	if err != nil {
		zap.S().Fatalf("failed to create protocol %s", err)
	}

	flag.StringVar(&inventory, "i", "", "")
	flag.StringVar(&sourceID, "sourceID", "", "")

	flag.Parse()

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

		e := cloudevents.NewEvent()
		e.SetID(uuid.New().String())
		e.SetType("assisted.migrations.events.inventory")
		e.SetSource("com.redhat.assisted-migration")
		e.SetExtension("sourceID", sourceID)
		_ = e.SetData(cloudevents.ApplicationJSON, inv)

		if result := c.Send(
			// Set the producer message key
			kafka_sarama.WithMessageKey(context.Background(), sarama.StringEncoder(e.ID())),
			e,
		); cloudevents.IsUndelivered(result) {
			zap.S().Infof("failed to send: %v", err)
		} else {
			zap.S().Infof("sent: %v, accepted: %t", now, cloudevents.IsACK(result))
		}

		<-time.After(3 * time.Second)
	}
}
