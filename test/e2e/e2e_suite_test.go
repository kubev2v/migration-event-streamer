package main

import (
	"fmt"
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
})
