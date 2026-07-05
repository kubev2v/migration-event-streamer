package main

import (
	"fmt"
	"time"

	"github.com/kubev2v/migration-event-streamer/test/e2e/service"
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

	ginkgo.Context("user action pipeline", func() {
		ginkgo.It("should index visited document when assessments are listed", func() {
			err := plannerSvc.ListAssessments()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// auth=none injects username="partner1", org="internal"
			expectedID := "internal_partner1_" + time.Now().UTC().Format("20060102")

			var doc *service.UserActionDocument
			gomega.Eventually(func() error {
				var getErr error
				doc, getErr = esSvc.GetUserAction(expectedID)
				return getErr
			}, 60*time.Second, 2*time.Second).Should(gomega.Succeed())

			gomega.Expect(doc.ActionType).To(gomega.Equal("visited"))
			gomega.Expect(doc.Username).To(gomega.Equal("partner1"))
			gomega.Expect(doc.OrgID).To(gomega.Equal("internal"))
			gomega.Expect(doc.Timestamp).ToNot(gomega.BeEmpty())
		})
	})

	ginkgo.Context("user action estimation pipeline", ginkgo.Ordered, func() {
		var assessmentID string

		ginkgo.BeforeAll(func() {
			inventory := buildTestInventory()
			assessment, err := plannerSvc.CreateAssessment("estimation-test-assessment", "inventory", nil, inventory)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			assessmentID = assessment.Id.String()

			// Wait for assessment to be indexed
			gomega.Eventually(func() error {
				_, err := esSvc.GetAssessment(assessmentID)
				return err
			}, 60*time.Second, 2*time.Second).Should(gomega.Succeed())
		})

		ginkgo.It("should index complexity estimated document", func() {
			err := plannerSvc.CalculateComplexity(assessmentID, "cluster-1")
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			var docs []service.UserActionDocument
			gomega.Eventually(func() error {
				if err := esSvc.RefreshIndex("migration_planner_user_action"); err != nil {
					return err
				}
				var searchErr error
				docs, searchErr = esSvc.SearchUserActions("action_type", "complexity_estimated")
				if searchErr != nil {
					return searchErr
				}
				for _, d := range docs {
					if d.AssessmentID == assessmentID {
						return nil
					}
				}
				return fmt.Errorf("waiting for complexity_estimated document")
			}, 60*time.Second, 2*time.Second).Should(gomega.Succeed())

			var found *service.UserActionDocument
			for i := range docs {
				if docs[i].AssessmentID == assessmentID {
					found = &docs[i]
					break
				}
			}
			gomega.Expect(found).ToNot(gomega.BeNil())
			gomega.Expect(found.ActionType).To(gomega.Equal("complexity_estimated"))
			gomega.Expect(found.Username).ToNot(gomega.BeEmpty())
			gomega.Expect(found.Timestamp).ToNot(gomega.BeEmpty())
		})

		ginkgo.It("should index time estimated document", func() {
			err := plannerSvc.CalculateTimeEstimation(assessmentID, "cluster-1")
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			gomega.Eventually(func() bool {
				_ = esSvc.RefreshIndex("migration_planner_user_action")
				docs, err := esSvc.SearchUserActions("action_type", "time_estimated")
				if err != nil {
					return false
				}
				for _, d := range docs {
					if d.AssessmentID == assessmentID {
						return true
					}
				}
				return false
			}, 60*time.Second, 2*time.Second).Should(gomega.BeTrue())
		})

	})
})
