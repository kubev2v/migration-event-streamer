package pipeline_test

import (
	"context"
	"encoding/json"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/pipeline"
)

type fakePipeline struct {
	started  bool
	input    <-chan entity.PipelineJob
	done     chan struct{}
	received chan entity.PipelineJob
}

func newFakePipeline(input <-chan entity.PipelineJob) *fakePipeline {
	return &fakePipeline{input: input, done: make(chan struct{}), received: make(chan entity.PipelineJob, 10)}
}

func (f *fakePipeline) Start(ctx context.Context) <-chan struct{} {
	f.started = true
	go func() {
		defer close(f.done)
		for {
			select {
			case job, ok := <-f.input:
				if !ok {
					return
				}
				f.received <- job
			case <-ctx.Done():
				return
			}
		}
	}()
	return f.done
}

func makeMessage(ceType string) entity.Message {
	e := cloudevents.NewEvent()
	e.SetType(ceType)
	e.SetSource("test")
	e.SetID("test-id")
	data, _ := json.Marshal(testInput{Value: "hello"})
	_ = e.SetData(cloudevents.ApplicationJSON, data)
	return entity.NewMessage(e)
}

var _ = Describe("Dispatcher", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		input  chan entity.Message
		errs   chan entity.PipelineError
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		input = make(chan entity.Message)
		errs = make(chan entity.PipelineError, 10)
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Register", func() {
		It("should register a pipeline that gets started on Start", func() {
			d := pipeline.NewDispatcher(input, errs)
			pipeInput := make(chan entity.PipelineJob)
			fp := newFakePipeline(pipeInput)

			d.Register("assessment.created", pipeInput, fp)

			_ = d.Start(ctx)
			Expect(fp.started).To(BeTrue())
		})
	})

	Describe("Start", func() {
		Context("when a message matches a registered pipeline", func() {
			It("should dispatch the job and wait for completion before committing", func() {
				d := pipeline.NewDispatcher(input, errs)
				pipeInput := make(chan entity.PipelineJob, 1)
				fp := newFakePipeline(pipeInput)
				d.Register("assessment.created", pipeInput, fp)

				_ = d.Start(ctx)

				msg := makeMessage("assisted.migration.assessment.created")
				go func() { input <- msg }()

				var job entity.PipelineJob
				Eventually(fp.received).Should(Receive(&job))

				Consistently(msg.CommitCh, 100*time.Millisecond).ShouldNot(BeClosed())

				close(job.Done)

				Eventually(msg.CommitCh).Should(BeClosed())
			})
		})

		Context("when the input channel is closed", func() {
			It("should stop the dispatcher", func() {
				d := pipeline.NewDispatcher(input, errs)
				pipeInput := make(chan entity.PipelineJob, 1)
				fp := newFakePipeline(pipeInput)
				d.Register("assessment.created", pipeInput, fp)

				done := d.Start(ctx)

				close(input)
				Eventually(done).Should(BeClosed())
			})
		})

		Context("when the context is cancelled", func() {
			It("should stop the dispatcher", func() {
				d := pipeline.NewDispatcher(input, errs)
				done := d.Start(ctx)

				cancel()
				Eventually(done).Should(BeClosed())
			})
		})

		Context("when multiple pipelines are registered", func() {
			It("should route messages to the correct pipeline", func() {
				d := pipeline.NewDispatcher(input, errs)

				assessmentInput := make(chan entity.PipelineJob, 1)
				fpAssessment := newFakePipeline(assessmentInput)
				d.Register("assessment.created", assessmentInput, fpAssessment)

				userInput := make(chan entity.PipelineJob, 1)
				fpUser := newFakePipeline(userInput)
				d.Register("user.updated", userInput, fpUser)

				_ = d.Start(ctx)

				msg := makeMessage("assisted.migration.user.updated")
				go func() { input <- msg }()

				var job entity.PipelineJob
				Eventually(fpUser.received).Should(Receive(&job))
				Consistently(fpAssessment.received, 50*time.Millisecond).ShouldNot(Receive())
				close(job.Done)
				Eventually(msg.CommitCh).Should(BeClosed())
			})
		})
	})
})
