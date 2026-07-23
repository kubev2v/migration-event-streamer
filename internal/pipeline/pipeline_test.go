package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/kubev2v/migration-event-streamer/internal/entity"
	"github.com/kubev2v/migration-event-streamer/internal/pipeline"
)

type testInput struct {
	Value string `json:"value"`
}

type testOutput struct {
	Result string
}

func drainErrors(ch <-chan entity.PipelineError) func() []entity.PipelineError {
	var (
		mu     sync.Mutex
		errors []entity.PipelineError
	)
	go func() {
		for pe := range ch {
			mu.Lock()
			errors = append(errors, pe)
			mu.Unlock()
			close(pe.Ack)
		}
	}()
	return func() []entity.PipelineError {
		mu.Lock()
		defer mu.Unlock()
		result := make([]entity.PipelineError, len(errors))
		copy(result, errors)
		return result
	}
}

var _ = Describe("Pipeline", func() {
	Describe("NewPipeline", func() {
		It("should return a non-nil pipeline", func() {
			input := make(chan entity.PipelineJob)
			errs := make(chan entity.PipelineError, 1)
			p := pipeline.NewPipeline[testInput, testOutput](
				"test-pipeline",
				func(_ context.Context, _ testInput) (testOutput, error) { return testOutput{}, nil },
				func(_ context.Context, _ testOutput) error { return nil },
				input, errs,
			)
			Expect(p).NotTo(BeNil())
		})
	})

	Describe("Start", func() {
		var (
			ctx       context.Context
			cancel    context.CancelFunc
			input     chan entity.PipelineJob
			errs      chan entity.PipelineError
			getErrors func() []entity.PipelineError
		)

		BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())
			input = make(chan entity.PipelineJob)
			errs = make(chan entity.PipelineError, 10)
			getErrors = drainErrors(errs)
		})

		AfterEach(func() {
			cancel()
		})

		Context("when processing a valid job", func() {
			It("should unmarshal, process, write, and close job.Done", func() {
				var processed testInput
				var written testOutput
				p := pipeline.NewPipeline[testInput, testOutput](
					"test",
					func(_ context.Context, in testInput) (testOutput, error) {
						processed = in
						return testOutput{Result: in.Value + "-processed"}, nil
					},
					func(_ context.Context, out testOutput) error {
						written = out
						return nil
					},
					input, errs,
				)
				done := p.Start(ctx)

				data, _ := json.Marshal(testInput{Value: "hello"})
				job := entity.NewPipelineJob(data)
				input <- job

				Eventually(job.Done).Should(BeClosed())
				Expect(processed.Value).To(Equal("hello"))
				Expect(written.Result).To(Equal("hello-processed"))
				Expect(getErrors()).To(BeEmpty())

				cancel()
				Eventually(done).Should(BeClosed())
			})
		})

		Context("when the job contains invalid JSON", func() {
			It("should send an error and still close job.Done", func() {
				p := pipeline.NewPipeline[testInput, testOutput](
					"test",
					func(_ context.Context, _ testInput) (testOutput, error) {
						Fail("processor should not be called on unmarshal error")
						return testOutput{}, nil
					},
					func(_ context.Context, _ testOutput) error { return nil },
					input, errs,
				)
				done := p.Start(ctx)

				job := entity.NewPipelineJob([]byte("not-json"))
				input <- job

				Eventually(job.Done).Should(BeClosed())
				Eventually(func() []entity.PipelineError { return getErrors() }).Should(HaveLen(1))
				Expect(getErrors()[0].Pipeline).To(Equal("test"))

				cancel()
				Eventually(done).Should(BeClosed())
			})
		})

		Context("when the processor returns an error", func() {
			It("should send an error and close job.Done", func() {
				p := pipeline.NewPipeline[testInput, testOutput](
					"test",
					func(_ context.Context, _ testInput) (testOutput, error) {
						return testOutput{}, errors.New("process failed")
					},
					func(_ context.Context, _ testOutput) error {
						Fail("writer should not be called on process error")
						return nil
					},
					input, errs,
				)
				done := p.Start(ctx)

				data, _ := json.Marshal(testInput{Value: "hello"})
				job := entity.NewPipelineJob(data)
				input <- job

				Eventually(job.Done).Should(BeClosed())
				Eventually(func() []entity.PipelineError { return getErrors() }).Should(HaveLen(1))
				Expect(getErrors()[0].Err.Error()).To(Equal("process failed"))

				cancel()
				Eventually(done).Should(BeClosed())
			})
		})

		Context("when the writer returns an error", func() {
			It("should send an error and close job.Done", func() {
				p := pipeline.NewPipeline[testInput, testOutput](
					"test",
					func(_ context.Context, in testInput) (testOutput, error) {
						return testOutput{Result: in.Value}, nil
					},
					func(_ context.Context, _ testOutput) error {
						return errors.New("write failed")
					},
					input, errs,
				)
				done := p.Start(ctx)

				data, _ := json.Marshal(testInput{Value: "hello"})
				job := entity.NewPipelineJob(data)
				input <- job

				Eventually(job.Done).Should(BeClosed())
				Eventually(func() []entity.PipelineError { return getErrors() }).Should(HaveLen(1))
				Expect(getErrors()[0].Err.Error()).To(Equal("write failed"))

				cancel()
				Eventually(done).Should(BeClosed())
			})
		})

		Context("when multiple jobs are sent", func() {
			It("should process all jobs sequentially", func() {
				var mu sync.Mutex
				var results []string
				p := pipeline.NewPipeline[testInput, testOutput](
					"test",
					func(_ context.Context, in testInput) (testOutput, error) {
						return testOutput{Result: in.Value}, nil
					},
					func(_ context.Context, out testOutput) error {
						mu.Lock()
						results = append(results, out.Result)
						mu.Unlock()
						return nil
					},
					input, errs,
				)
				done := p.Start(ctx)

				for _, v := range []string{"a", "b", "c"} {
					data, _ := json.Marshal(testInput{Value: v})
					job := entity.NewPipelineJob(data)
					input <- job
					Eventually(job.Done).Should(BeClosed())
				}

				Expect(results).To(Equal([]string{"a", "b", "c"}))
				Expect(getErrors()).To(BeEmpty())

				cancel()
				Eventually(done).Should(BeClosed())
			})
		})

		Context("when the input channel is closed", func() {
			It("should stop the pipeline goroutine", func() {
				p := pipeline.NewPipeline[testInput, testOutput](
					"test",
					func(_ context.Context, in testInput) (testOutput, error) {
						return testOutput{}, nil
					},
					func(_ context.Context, _ testOutput) error { return nil },
					input, errs,
				)
				done := p.Start(ctx)

				close(input)
				Eventually(done).Should(BeClosed())
			})
		})

		Context("when the context is cancelled", func() {
			It("should stop the pipeline goroutine", func() {
				p := pipeline.NewPipeline[testInput, testOutput](
					"test",
					func(_ context.Context, in testInput) (testOutput, error) {
						return testOutput{}, nil
					},
					func(_ context.Context, _ testOutput) error { return nil },
					input, errs,
				)
				done := p.Start(ctx)

				cancel()
				Eventually(done).Should(BeClosed())
			})
		})
	})

	Describe("WithRetry", func() {
		var (
			ctx       context.Context
			cancel    context.CancelFunc
			input     chan entity.PipelineJob
			errs      chan entity.PipelineError
			getErrors func() []entity.PipelineError
		)

		BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())
			input = make(chan entity.PipelineJob)
			errs = make(chan entity.PipelineError, 10)
			getErrors = drainErrors(errs)
		})

		AfterEach(func() {
			cancel()
		})

		It("should succeed without retry when no error occurs", func() {
			var callCount int
			p := pipeline.NewPipeline[testInput, testOutput](
				"test",
				func(_ context.Context, in testInput) (testOutput, error) {
					callCount++
					return testOutput{Result: in.Value}, nil
				},
				func(_ context.Context, _ testOutput) error { return nil },
				input, errs,
			).WithRetry(3, 0)
			done := p.Start(ctx)

			data, _ := json.Marshal(testInput{Value: "hello"})
			job := entity.NewPipelineJob(data)
			input <- job

			Eventually(job.Done).Should(BeClosed())
			Expect(callCount).To(Equal(1))
			Expect(getErrors()).To(BeEmpty())

			cancel()
			Eventually(done).Should(BeClosed())
		})

		It("should retry and succeed after transient failures", func() {
			var callCount int
			p := pipeline.NewPipeline[testInput, testOutput](
				"test",
				func(_ context.Context, in testInput) (testOutput, error) {
					callCount++
					if callCount < 3 {
						return testOutput{}, errors.New("transient error")
					}
					return testOutput{Result: in.Value}, nil
				},
				func(_ context.Context, _ testOutput) error { return nil },
				input, errs,
			).WithRetry(3, 0)
			done := p.Start(ctx)

			data, _ := json.Marshal(testInput{Value: "hello"})
			job := entity.NewPipelineJob(data)
			input <- job

			Eventually(job.Done).Should(BeClosed())
			Expect(callCount).To(Equal(3))
			Expect(getErrors()).To(BeEmpty())

			cancel()
			Eventually(done).Should(BeClosed())
		})

		It("should return error after exhausting all retries", func() {
			var callCount int
			p := pipeline.NewPipeline[testInput, testOutput](
				"test",
				func(_ context.Context, _ testInput) (testOutput, error) {
					callCount++
					return testOutput{}, errors.New("persistent error")
				},
				func(_ context.Context, _ testOutput) error { return nil },
				input, errs,
			).WithRetry(3, 0)
			done := p.Start(ctx)

			data, _ := json.Marshal(testInput{Value: "hello"})
			job := entity.NewPipelineJob(data)
			input <- job

			Eventually(job.Done).Should(BeClosed())
			Expect(callCount).To(Equal(4)) // 1 initial + 3 retries
			Eventually(func() []entity.PipelineError { return getErrors() }).Should(HaveLen(1))
			Expect(getErrors()[0].Err.Error()).To(Equal("persistent error"))

			cancel()
			Eventually(done).Should(BeClosed())
		})

		It("should stop retrying when context is cancelled", func() {
			var callCount int
			p := pipeline.NewPipeline[testInput, testOutput](
				"test",
				func(fnCtx context.Context, _ testInput) (testOutput, error) {
					callCount++
					if callCount == 1 {
						cancel()
					}
					return testOutput{}, errors.New("will cancel")
				},
				func(_ context.Context, _ testOutput) error { return nil },
				input, errs,
			).WithRetry(3, 1000*time.Millisecond)
			done := p.Start(ctx)

			data, _ := json.Marshal(testInput{Value: "hello"})
			job := entity.NewPipelineJob(data)
			input <- job

			Eventually(done).Should(BeClosed())
			Expect(callCount).To(Equal(1))
		})
	})

	Describe("sendError", func() {
		It("should block until the error is acknowledged", func() {
			input := make(chan entity.PipelineJob)
			errs := make(chan entity.PipelineError, 1)
			p := pipeline.NewPipeline[testInput, testOutput](
				"test-pipeline",
				func(_ context.Context, _ testInput) (testOutput, error) {
					return testOutput{}, errors.New("some error")
				},
				func(_ context.Context, _ testOutput) error { return nil },
				input, errs,
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := p.Start(ctx)

			data, _ := json.Marshal(testInput{Value: "x"})
			job := entity.NewPipelineJob(data)

			go func() { input <- job }()

			var pe entity.PipelineError
			Eventually(errs).Should(Receive(&pe))
			Expect(pe.Pipeline).To(Equal("test-pipeline"))
			Expect(pe.Err.Error()).To(Equal("some error"))

			// job.Done should not be closed yet because sendError is blocking on Ack
			Consistently(job.Done, 100*time.Millisecond).ShouldNot(BeClosed())

			close(pe.Ack)
			Eventually(job.Done).Should(BeClosed())

			cancel()
			Eventually(done).Should(BeClosed())
		})
	})
})
