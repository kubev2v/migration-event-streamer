package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"go.uber.org/zap"
)

// GrantConsumerACLs allows the given principal to consume from and produce to
// the provided topics and consumer groups. It grants Read, Write, and Describe
// on each topic, plus Read and Describe on each group, which is what a franz-go
// client needs to produce records and consume/commit within its group.
//
// The principal is a Kafka principal such as the SASL/SCRAM username; a bare
// username is automatically prefixed with "User:". Creating an ACL that already
// exists is a no-op on the broker, so this is safe to run on every init.
func GrantConsumerACLs(ctx context.Context, adm *kadm.Client, principal string, topics, groups []string) error {
	if principal == "" {
		return fmt.Errorf("cannot grant ACLs: empty principal")
	}
	if len(topics) == 0 && len(groups) == 0 {
		return nil
	}

	b := kadm.NewACLs().
		Allow(principal).
		AllowHosts("*").
		Operations(kadm.OpRead, kadm.OpWrite, kadm.OpDescribe).
		ResourcePatternType(kadm.ACLPatternLiteral)
	b.PrefixUser()

	if len(topics) > 0 {
		b.Topics(topics...)
	}
	if len(groups) > 0 {
		b.Groups(groups...)
	}

	results, err := adm.CreateACLs(ctx, b)
	if err != nil {
		return fmt.Errorf("failed to create ACLs for principal %s: %w", principal, err)
	}

	for _, r := range results {
		if r.Err != nil {
			return fmt.Errorf("failed to create ACL (%s %s %v) for principal %s: %w",
				r.Type, r.Name, r.Operation, principal, r.Err)
		}
	}

	zap.S().Infow("consumer ACLs granted", "principal", principal, "topics", topics, "groups", groups)
	return nil
}
