package main

import (
	"fmt"
	"github.com/kubev2v/migration-event-streamer/test/e2e/service"
	"testing"

	"github.com/kubev2v/migration-event-streamer/test/e2e/infra"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"go.uber.org/zap"
)

func TestE2E(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "E2E Suite")
}

var _ = ginkgo.BeforeSuite(func() {
	logger, err := zap.NewDevelopment()
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	zap.ReplaceGlobals(logger)

	gomega.Expect(cfg.PlannerImage).ToNot(gomega.BeEmpty(), "planner-image is required")
	gomega.Expect(cfg.StreamerImage).ToNot(gomega.BeEmpty(), "streamer-image is required")

	im, err := infra.NewContainerInfraManager(cfg.PodmanSocket, cfg.PlannerImage, cfg.StreamerImage)
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), fmt.Sprintf("failed to create infra manager: %v", err))
	infraManager = im

	gomega.Expect(infraManager.StartPostgres()).To(gomega.Succeed())
	gomega.Expect(infraManager.StartKafka()).To(gomega.Succeed())
	gomega.Expect(infraManager.StartElasticsearch()).To(gomega.Succeed())
	gomega.Expect(infraManager.CreateKafkaTopics()).To(gomega.Succeed())
	gomega.Expect(infraManager.StartPlanner()).To(gomega.Succeed())
	gomega.Expect(infraManager.StartStreamer()).To(gomega.Succeed())

	plannerSvc = service.NewPlannerService("http://localhost:3443")

	esSvc, err = service.NewElasticsearchService("http://localhost:9200")
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

})

var _ = ginkgo.AfterSuite(func() {
	if infraManager != nil {
		_ = infraManager.StopStreamer()
		_ = infraManager.StopPlanner()
		_ = infraManager.StopElasticsearch()
		_ = infraManager.StopKafka()
		_ = infraManager.StopPostgres()
	}
})
