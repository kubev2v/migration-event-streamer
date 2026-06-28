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
	plannerEvents "github.com/kubev2v/migration-planner/pkg/events"
	"go.uber.org/zap"
)

const (
	assessmentCreatedEventType = "assisted.migration.assessment.created"
	visitorEventType           = "assisted.migration.user_action.visited"
	partnerCustomerEventType   = "assisted.migration.partner_customer.updated"
	userActionEventType        = "assisted.migration.user_action.assessment_shared"
	inputTopic                 = "assisted.migration.events"
	eventSource                = "com.redhat.assisted-migration"
)

var (
	inventory string
	sourceID  string
	timeout   string
	oneShot   bool
)

func main() {
	logger := logger.SetupLogger("console", "debug")
	defer func() {
		_ = logger.Sync()
	}()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	flag.StringVar(&inventory, "inventory", "", "")
	flag.StringVar(&sourceID, "source_id", uuid.NewString(), "")
	flag.StringVar(&timeout, "timeout", "1s", "")
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
	inv := make(map[string]any)
	if err := json.Unmarshal(data, &inv); err != nil {
		zap.S().Fatal(err)
	}

	producer, err := plannerEvents.NewKafkaProducer([]string{"127.0.0.1:9092"})
	if err != nil {
		zap.S().Fatalf("failed to create producer: %s", err)
	}
	defer producer.Close()

	for {
		who := rand.Intn(3) + 1
		e := cloudevents.NewEvent()
		e.SetID(uuid.New().String())
		e.SetSource(eventSource)
		now := time.Now()
		switch who {
		case 1:
			e.SetType(assessmentCreatedEventType)
			e.SetExtension("sourceID", sourceID)
			payload := map[string]any{
				"assessment": map[string]any{
					"id":          uuid.NewString(),
					"snapshot_id": 1,
					"name":        "test-assessment",
					"org_id":      "test-org",
					"username":    "testuser",
					"source_type": "vsphere",
					"inventory":   inv,
					"created_at":  now,
				},
			}
			_ = e.SetData(cloudevents.ApplicationJSON, payload)
		case 2:
			e.SetType(partnerCustomerEventType)
			payload := map[string]any{
				"partner_customer": map[string]any{
					"id":                uuid.NewString(),
					"customer_username": "testuser",
					"partner_id":        "partner-123",
					"request_status":    "accepted",
					"location":          "us-east-1",
					"created_at":        now,
				},
			}
			_ = e.SetData(cloudevents.ApplicationJSON, payload)
		case 3:
			e.SetType(userActionEventType)
			payload := map[string]any{
				"user_action": map[string]any{
					"username":  "testuser",
					"timestamp": now,
					"data": map[string]any{
						"assessment_id": uuid.NewString(),
						"partner_id":    "partner-123",
					},
				},
			}
			_ = e.SetData(cloudevents.ApplicationJSON, payload)
		}

		data, merr := json.Marshal(e)
		if merr != nil {
			zap.S().Errorf("failed to marshal event: %v", merr)
			continue
		}

		if err := producer.Write(context.Background(), inputTopic, data); err != nil {
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
