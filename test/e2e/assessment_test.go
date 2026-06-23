package main

import (
	"time"

	"github.com/kubev2v/migration-event-streamer/test/e2e/service"
	"github.com/kubev2v/migration-planner/api/v1alpha1"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Migration Event Streamer E2E", ginkgo.Ordered, func() {
	var (
		plannerSvc *service.PlannerService
		esSvc      *service.ElasticsearchService
	)

	ginkgo.BeforeAll(func() {
		gomega.Expect(infraManager.StartPostgres()).To(gomega.Succeed())
		gomega.Expect(infraManager.StartKafka()).To(gomega.Succeed())
		gomega.Expect(infraManager.StartElasticsearch()).To(gomega.Succeed())
		gomega.Expect(infraManager.CreateKafkaTopics()).To(gomega.Succeed())
		gomega.Expect(infraManager.StartPlanner()).To(gomega.Succeed())
		gomega.Expect(infraManager.StartStreamer()).To(gomega.Succeed())

		plannerSvc = service.NewPlannerService("http://localhost:3443")

		var err error
		esSvc, err = service.NewElasticsearchService("http://localhost:9200")
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
	})

	ginkgo.AfterAll(func() {
		_ = infraManager.StopStreamer()
		_ = infraManager.StopPlanner()
		_ = infraManager.StopElasticsearch()
		_ = infraManager.StopKafka()
		_ = infraManager.StopPostgres()
	})

	ginkgo.AfterEach(func() {
		if ginkgo.CurrentSpecReport().Failed() {
			ginkgo.GinkgoWriter.Println("=== Streamer Logs ===")
			ginkgo.GinkgoWriter.Println(infraManager.StreamerLogs())
			ginkgo.GinkgoWriter.Println("=== Planner Logs ===")
			ginkgo.GinkgoWriter.Println(infraManager.PlannerLogs())
		}
	})

	ginkgo.Context("assessment pipeline", func() {
		ginkgo.It("should index assessment document in elasticsearch when assessment is created", func() {
			inventory := buildTestInventory()
			assessment, err := plannerSvc.CreateAssessment("test-assessment", "inventory", nil, inventory)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(assessment).ToNot(gomega.BeNil())

			var doc *service.AssessmentDocument
			gomega.Eventually(func() error {
				var getErr error
				doc, getErr = esSvc.GetAssessment(assessment.Id.String())
				return getErr
			}, 60*time.Second, 2*time.Second).Should(gomega.Succeed())

			gomega.Expect(doc.Name).To(gomega.Equal("test-assessment"))
			gomega.Expect(doc.Status).To(gomega.Equal("active"))
			gomega.Expect(doc.SourceType).To(gomega.Equal("inventory"))
			gomega.Expect(doc.OrgID).ToNot(gomega.BeEmpty())
			gomega.Expect(doc.TotalVMs).To(gomega.Equal(10))
			gomega.Expect(doc.TotalHosts).To(gomega.Equal(2))
		})
	})
})

func buildTestInventory() *v1alpha1.Inventory {
	totalDatacenters := 1

	return &v1alpha1.Inventory{
		VcenterId: "vcenter-test-001",
		Clusters:  map[string]v1alpha1.InventoryData{},
		Vcenter: &v1alpha1.InventoryData{
			Vms: v1alpha1.VMs{
				Total:                10,
				TotalMigratable:      8,
				CpuCores:             v1alpha1.VMResourceBreakdown{Total: 10},
				DiskCount:            v1alpha1.VMResourceBreakdown{Total: 10},
				DiskGB:               v1alpha1.VMResourceBreakdown{Total: 10},
				RamGB:                v1alpha1.VMResourceBreakdown{Total: 10},
				MigrationWarnings:    []v1alpha1.MigrationIssue{},
				NotMigratableReasons: []v1alpha1.MigrationIssue{},
				PowerStates:          map[string]int{"poweredOn": 10},
			},
			Infra: v1alpha1.Infra{
				TotalHosts:       2,
				TotalDatacenters: &totalDatacenters,
				Datastores:       []v1alpha1.Datastore{},
				Networks:         []v1alpha1.Network{},
				HostPowerStates:  map[string]int{"poweredOn": 2},
			},
		},
	}
}
