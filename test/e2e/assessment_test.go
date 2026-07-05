package main

import (
	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"time"

	"fmt"

	"github.com/kubev2v/migration-planner/api/v1alpha1"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Migration Event Streamer E2E", func() {

	ginkgo.AfterEach(func() {
		if ginkgo.CurrentSpecReport().Failed() {
			ginkgo.GinkgoWriter.Println("=== Streamer Logs ===")
			ginkgo.GinkgoWriter.Println(infraManager.StreamerLogs())
			ginkgo.GinkgoWriter.Println("=== Planner Logs ===")
			ginkgo.GinkgoWriter.Println(infraManager.PlannerLogs())
		}
	})

	ginkgo.Context("assessment pipeline", ginkgo.Ordered, func() {
		var assessmentID string

		ginkgo.It("should index assessment, OS, and datastore documents when assessment is created", func() {
			inventory := buildTestInventory()
			assessment, err := plannerSvc.CreateAssessment("test-assessment", "inventory", nil, inventory)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(assessment).ToNot(gomega.BeNil())
			assessmentID = assessment.Id.String()

			var doc *entity.Assessment
			gomega.Eventually(func() error {
				var getErr error
				doc, getErr = esSvc.GetAssessment(assessmentID)
				return getErr
			}, 60*time.Second, 2*time.Second).Should(gomega.Succeed())

			gomega.Expect(doc.Name).To(gomega.Equal("test-assessment"))
			gomega.Expect(doc.Status).To(gomega.Equal("active"))
			gomega.Expect(doc.SourceType).To(gomega.Equal("inventory"))
			gomega.Expect(doc.OrgID).ToNot(gomega.BeEmpty())
			gomega.Expect(doc.TotalVMs).To(gomega.Equal(10))
			gomega.Expect(doc.TotalHosts).To(gomega.Equal(2))

			// Verify OS documents
			var osDocs []entity.AssessmentOS
			gomega.Eventually(func() error {
				if err := esSvc.RefreshIndex("migration_planner_os"); err != nil {
					return err
				}
				var searchErr error
				osDocs, searchErr = esSvc.SearchOSDocuments(assessmentID)
				if searchErr != nil {
					return searchErr
				}
				if len(osDocs) < 2 {
					return fmt.Errorf("waiting for OS documents")
				}
				return nil
			}, 60*time.Second, 2*time.Second).Should(gomega.Succeed())

			gomega.Expect(osDocs).To(gomega.HaveLen(2))
			osTypes := map[string]int{}
			for _, d := range osDocs {
				osTypes[d.OSType] = d.VMCount
				gomega.Expect(d.Status).To(gomega.Equal("active"))
				gomega.Expect(d.AssessmentID).To(gomega.Equal(assessmentID))
			}
			gomega.Expect(osTypes).To(gomega.Equal(map[string]int{"windows": 5, "linux": 3}))

			// Verify datastore documents
			var dsDocs []entity.AssessmentDatastore
			gomega.Eventually(func() error {
				if err := esSvc.RefreshIndex("migration_planner_datastore"); err != nil {
					return err
				}
				var searchErr error
				dsDocs, searchErr = esSvc.SearchDatastoreDocuments(assessmentID)
				if searchErr != nil {
					return searchErr
				}
				if len(dsDocs) < 2 {
					return fmt.Errorf("waiting for datastore documents")
				}
				return nil
			}, 60*time.Second, 2*time.Second).Should(gomega.Succeed())

			gomega.Expect(dsDocs).To(gomega.HaveLen(2))
			dsTypes := map[string]int{}
			for _, d := range dsDocs {
				dsTypes[d.DatastoreType] = d.TotalCapacityGB
				gomega.Expect(d.Status).To(gomega.Equal("active"))
				gomega.Expect(d.AssessmentID).To(gomega.Equal(assessmentID))
			}
			gomega.Expect(dsTypes).To(gomega.Equal(map[string]int{"VMFS": 1000, "NFS": 2000}))
		})

		ginkgo.It("should not cross-contaminate when a second assessment is created", func() {
			inventory := buildTestInventory()
			assessment2, err := plannerSvc.CreateAssessment("test-assessment-2", "inventory", nil, inventory)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			assessment2ID := assessment2.Id.String()

			var doc2 *entity.Assessment
			gomega.Eventually(func() error {
				var getErr error
				doc2, getErr = esSvc.GetAssessment(assessment2ID)
				return getErr
			}, 60*time.Second, 2*time.Second).Should(gomega.Succeed())

			gomega.Expect(doc2.Name).To(gomega.Equal("test-assessment-2"))

			// Original assessment should still exist unchanged
			doc1, err := esSvc.GetAssessment(assessmentID)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(doc1.Name).To(gomega.Equal("test-assessment"))
		})

		ginkgo.It("should soft-delete assessment, OS, and datastore documents when assessment is deleted", func() {
			err := plannerSvc.DeleteAssessment(assessmentID)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// Wait for assessment to be marked as deleted
			gomega.Eventually(func() string {
				doc, err := esSvc.GetAssessment(assessmentID)
				if err != nil {
					return ""
				}
				return doc.Status
			}, 60*time.Second, 2*time.Second).Should(gomega.Equal("deleted"))

			doc, err := esSvc.GetAssessment(assessmentID)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(doc.DeletedAt).ToNot(gomega.BeNil())

			// Verify OS documents are soft-deleted
			gomega.Eventually(func() bool {
				_ = esSvc.RefreshIndex("migration_planner_os")
				osDocs, err := esSvc.SearchOSDocuments(assessmentID)
				if err != nil || len(osDocs) == 0 {
					return false
				}
				for _, d := range osDocs {
					if d.Status != "deleted" {
						return false
					}
				}
				return true
			}, 60*time.Second, 2*time.Second).Should(gomega.BeTrue())

			// Verify datastore documents are soft-deleted
			gomega.Eventually(func() bool {
				_ = esSvc.RefreshIndex("migration_planner_datastore")
				dsDocs, err := esSvc.SearchDatastoreDocuments(assessmentID)
				if err != nil || len(dsDocs) == 0 {
					return false
				}
				for _, d := range dsDocs {
					if d.Status != "deleted" {
						return false
					}
				}
				return true
			}, 60*time.Second, 2*time.Second).Should(gomega.BeTrue())
		})
	})
})

func buildTestInventory() *v1alpha1.Inventory {
	totalDatacenters := 1
	osInfo := map[string]v1alpha1.OsInfo{
		"windows": {Count: 5, Supported: true},
		"linux":   {Count: 3, Supported: true},
	}
	diskSizeTier := map[string]v1alpha1.DiskSizeTierSummary{
		"0-10TiB":  {VmCount: 3, TotalSizeTB: 1.5},
		"10-20TiB": {VmCount: 2, TotalSizeTB: 15.0},
	}

	return &v1alpha1.Inventory{
		VcenterId: "vcenter-test-001",
		Clusters: map[string]v1alpha1.InventoryData{
			"cluster-1": {
				Infra: v1alpha1.Infra{
					TotalHosts: 1,
					Datastores: []v1alpha1.Datastore{
						{Type: "VMFS", TotalCapacityGB: 1000, FreeCapacityGB: 500, DiskId: "disk-1", Model: "test-model", ProtocolType: "scsi"},
						{Type: "NFS", TotalCapacityGB: 2000, FreeCapacityGB: 1500, DiskId: "disk-2", Model: "test-model", ProtocolType: "nfs"},
					},
					Networks:        []v1alpha1.Network{},
					HostPowerStates: map[string]int{"poweredOn": 1},
				},
				Vms: v1alpha1.VMs{
					Total:                5,
					TotalMigratable:      4,
					OsInfo:               &osInfo,
					DiskSizeTier:         &diskSizeTier,
					CpuCores:             v1alpha1.VMResourceBreakdown{Total: 5},
					DiskCount:            v1alpha1.VMResourceBreakdown{Total: 5},
					DiskGB:               v1alpha1.VMResourceBreakdown{Total: 5},
					RamGB:                v1alpha1.VMResourceBreakdown{Total: 5},
					MigrationWarnings:    []v1alpha1.MigrationIssue{},
					NotMigratableReasons: []v1alpha1.MigrationIssue{},
					PowerStates:          map[string]int{"poweredOn": 5},
				},
			},
		},
		Vcenter: &v1alpha1.InventoryData{
			Vms: v1alpha1.VMs{
				Total:                10,
				TotalMigratable:      8,
				OsInfo:               &osInfo,
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
