package elastic

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestElastic(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Elastic Suite")
}
