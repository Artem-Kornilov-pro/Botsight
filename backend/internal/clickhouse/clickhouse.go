// Package clickhouse opens connections to ClickHouse over the native protocol.
package clickhouse

import (
	"context"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Options struct {
	Addr     []string // host:port of the native protocol (9000)
	Database string
	Username string
	Password string
}

// Open connects and pings. LZ4 compression is cheap on CPU and cuts the network
// traffic of large inserts several times.
func Open(ctx context.Context, o Options) (driver.Conn, error) {
	conn, err := ch.Open(&ch.Options{
		Addr:             o.Addr,
		Auth:             ch.Auth{Database: o.Database, Username: o.Username, Password: o.Password},
		Compression:      &ch.Compression{Method: ch.CompressionLZ4},
		DialTimeout:      5 * time.Second,
		MaxOpenConns:     32,
		MaxIdleConns:     8,
		ConnMaxLifetime:  time.Hour,
		ConnOpenStrategy: ch.ConnOpenRoundRobin,
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}
