package elastic

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/kubev2v/migration-event-streamer/internal/config"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"go.uber.org/zap"
)

const (
	openSearchImage = "docker.io/opensearchproject/opensearch:2.19.6"
	openSearchPort  = 19200
	indexPrefix     = "unit_test"
)

var (
	openSearchContainer string
	openSearchBase      *baseWriter
)

var _ = BeforeSuite(func() {
	zap.ReplaceGlobals(zap.NewNop())

	openSearchContainer = fmt.Sprintf("migration-event-streamer-unit-opensearch-%d-%d", os.Getpid(), time.Now().UnixNano())
	containerStarted := false
	DeferCleanup(func() {
		if containerStarted {
			output, err := exec.Command("podman", "rm", "--force", openSearchContainer).CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), string(output))
		}
	})

	command := exec.Command("podman", "run", "--detach", "--rm", "--name", openSearchContainer,
		"--publish", fmt.Sprintf("%d:9200", openSearchPort),
		"--env", "discovery.type=single-node",
		"--env", "plugins.security.disabled=true",
		"--env", "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m",
		"--env", "DISABLE_INSTALL_DEMO_CONFIG=true",
		openSearchImage,
	)
	output, err := command.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), string(output))
	containerStarted = true
	GinkgoWriter.Printf("Started OpenSearch container %s (%s)\n", openSearchContainer, output)

	openSearchAddress := fmt.Sprintf("http://localhost:%d", openSearchPort)
	Eventually(func() error {
		client := http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get(openSearchAddress + "/_cluster/health")
		if err != nil {
			return err
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("OpenSearch health endpoint returned %s", resp.Status)
		}
		return nil
	}, 90*time.Second, 2*time.Second).Should(Succeed())

	client, err := NewElasticsearchClient(config.ElasticSearch{
		Host:            openSearchAddress,
		ResponseTimeout: "10s",
		DialTimeout:     "2s",
	})
	Expect(err).NotTo(HaveOccurred())
	openSearchBase = &baseWriter{client: client, indexPrefix: indexPrefix}
})

var _ = Describe("baseWriter", func() {
	It("writes a document that can be read back from OpenSearch", func() {
		ctx := context.Background()
		const index = "write_test"
		const documentID = "assessment-1"
		const document = `{"assessment_id":"assessment-1","status":"active"}`

		Expect(createTestIndex(ctx, index)).To(Succeed())
		Expect(openSearchBase.write(ctx, index, documentID, []byte(document))).To(Succeed())

		stored := getTestDocument(ctx, index, documentID)
		Expect(stored.Found).To(BeTrue())
		Expect(string(stored.Source)).To(MatchJSON(document))
	})

	It("replaces existing document fields when writing the same ID again", func() {
		ctx := context.Background()
		const index = "write_overwrite_test"
		const documentID = "assessment-1"

		Expect(createTestIndex(ctx, index)).To(Succeed())
		Expect(openSearchBase.write(ctx, index, documentID, []byte(`{"name":"original","status":"active"}`))).To(Succeed())
		Expect(openSearchBase.write(ctx, index, documentID, []byte(`{"status":"deleted"}`))).To(Succeed())

		stored := getTestDocument(ctx, index, documentID)
		Expect(stored.Found).To(BeTrue())
		Expect(string(stored.Source)).To(MatchJSON(`{"status":"deleted"}`))
	})

	It("upserts fields while preserving existing document fields", func() {
		ctx := context.Background()
		const index = "upsert_update_test"
		const documentID = "assessment-2"

		Expect(createTestIndex(ctx, index)).To(Succeed())
		Expect(openSearchBase.write(ctx, index, documentID, []byte(`{"name":"original","status":"active"}`))).To(Succeed())
		Expect(openSearchBase.upsert(ctx, index, documentID, []byte(`{"status":"deleted"}`))).To(Succeed())

		stored := getTestDocument(ctx, index, documentID)
		Expect(stored.Found).To(BeTrue())
		Expect(string(stored.Source)).To(MatchJSON(`{"name":"original","status":"deleted"}`))
	})

	It("creates a missing document through upsert", func() {
		ctx := context.Background()
		const index = "upsert_create_test"
		const documentID = "assessment-3"

		Expect(createTestIndex(ctx, index)).To(Succeed())
		Expect(openSearchBase.upsert(ctx, index, documentID, []byte(`{"name":"created by upsert","status":"active"}`))).To(Succeed())

		stored := getTestDocument(ctx, index, documentID)
		Expect(stored.Found).To(BeTrue())
		Expect(string(stored.Source)).To(MatchJSON(`{"name":"created by upsert","status":"active"}`))
	})

	It("updates all matching documents by query and leaves other documents unchanged", func() {
		ctx := context.Background()
		const index = "update_by_query_test"

		Expect(createTestIndex(ctx, index)).To(Succeed())
		Expect(openSearchBase.write(ctx, index, "match-1", []byte(`{"assessment_id":"assessment-4","status":"active"}`))).To(Succeed())
		Expect(openSearchBase.write(ctx, index, "match-2", []byte(`{"assessment_id":"assessment-4","status":"active"}`))).To(Succeed())
		Expect(openSearchBase.write(ctx, index, "other", []byte(`{"assessment_id":"assessment-5","status":"active"}`))).To(Succeed())
		Expect(refreshTestIndex(ctx, index)).To(Succeed())

		result, err := openSearchBase.updateByQuery(ctx, UpdateByQueryRequest{
			Index:      index,
			MatchField: "assessment_id.keyword",
			MatchValue: "assessment-4",
			Updates: map[string]any{
				"status":     "deleted",
				"deleted_at": "2026-09-27T00:00:00Z",
			},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Total).To(Equal(int64(2)))
		Expect(result.Updated).To(Equal(int64(2)))
		Expect(result.Failed).To(BeZero())

		Expect(refreshTestIndex(ctx, index)).To(Succeed())
		Expect(string(getTestDocument(ctx, index, "match-1").Source)).To(MatchJSON(`{"assessment_id":"assessment-4","status":"deleted","deleted_at":"2026-09-27T00:00:00Z"}`))
		Expect(string(getTestDocument(ctx, index, "match-2").Source)).To(MatchJSON(`{"assessment_id":"assessment-4","status":"deleted","deleted_at":"2026-09-27T00:00:00Z"}`))
		Expect(string(getTestDocument(ctx, index, "other").Source)).To(MatchJSON(`{"assessment_id":"assessment-5","status":"active"}`))
	})
})

func createTestIndex(ctx context.Context, index string) error {
	_, err := openSearchBase.client.Indices.Create(ctx, opensearchapi.IndicesCreateReq{
		Index: fmt.Sprintf("%s_%s", openSearchBase.indexPrefix, index),
	})
	return err
}

func refreshTestIndex(ctx context.Context, index string) error {
	_, err := openSearchBase.client.Indices.Refresh(ctx, &opensearchapi.IndicesRefreshReq{
		Index: []string{fmt.Sprintf("%s_%s", openSearchBase.indexPrefix, index)},
	})
	return err
}

func getTestDocument(ctx context.Context, index, documentID string) *opensearchapi.DocumentGetResp {
	stored, err := openSearchBase.client.Document.Get(ctx, opensearchapi.DocumentGetReq{
		Index:      fmt.Sprintf("%s_%s", openSearchBase.indexPrefix, index),
		DocumentID: documentID,
	})
	Expect(err).NotTo(HaveOccurred())
	return stored
}
