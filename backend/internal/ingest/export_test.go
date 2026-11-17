package ingest

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// WithEndTxn sets the hook that ends transactions in place of EndTransaction.
func WithEndTxn(cfg TxnConfig, f func(ctx context.Context, cl *kgo.Client, try kgo.TransactionEndTry) error) TxnConfig {
	cfg.endTxn = f
	return cfg
}
