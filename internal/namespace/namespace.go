package namespace

import (
	"os"
	"strings"
	"sync"
)

const namespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
const DefaultNamespace = "assisted-migration-local"

var Namespace = sync.OnceValue(func() string {
	if ns, err := os.ReadFile(namespacePath); err == nil && len(ns) > 0 {
		return strings.TrimSpace(string(ns))
	}
	return DefaultNamespace
})

func Topic() string {
	return Namespace() + ".events"
}

func IndexPrefix() string {
	return strings.ReplaceAll(Namespace(), "-", "_")
}
