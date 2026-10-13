// Package testenv starts the backing services in containers for integration tests
// (PostgreSQL, Redis, Kafka, ClickHouse) and applies the migrations from migrations/.
package testenv

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/clickhouse"
)

// Images match deploy/docker-compose.yml.
const (
	PostgresImage   = "postgres:17-alpine"
	RedisImage      = "redis:7.4-alpine"
	KafkaImage      = "apache/kafka:4.1.0"
	ClickHouseImage = "clickhouse/clickhouse-server:25.8"
)

// Options selects the services to start.
type Options struct {
	Postgres, Redis, Kafka, ClickHouse bool
}

type Env struct {
	Pool         *pgxpool.Pool
	Redis        *redis.Client
	KafkaBrokers []string
	ClickHouse   driver.Conn
	// ClickHouseAddr is host:port of the native protocol; database, user and password are "botsight".
	ClickHouseAddr string

	mu         sync.Mutex
	containers []testcontainers.Container
}

// Start runs PostgreSQL and Redis.
func Start(ctx context.Context) (*Env, error) {
	return StartWith(ctx, Options{Postgres: true, Redis: true})
}

// StartWith starts the selected services in parallel.
func StartWith(ctx context.Context, opts Options) (*Env, error) {
	env := &Env{}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	run := func(enabled bool, start func(context.Context) error) {
		if !enabled {
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := start(ctx); err != nil {
				errs <- err
			}
		}()
	}
	run(opts.Postgres, env.startPostgres)
	run(opts.Redis, env.startRedis)
	run(opts.Kafka, env.startKafka)
	run(opts.ClickHouse, env.startClickHouse)
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		env.Close()
		return nil, err
	}
	return env, nil
}

func (e *Env) track(c testcontainers.Container) {
	if c == nil {
		return
	}
	e.mu.Lock()
	e.containers = append(e.containers, c)
	e.mu.Unlock()
}

func (e *Env) Close() {
	if e.Pool != nil {
		e.Pool.Close()
	}
	if e.Redis != nil {
		_ = e.Redis.Close()
	}
	if e.ClickHouse != nil {
		_ = e.ClickHouse.Close()
	}
	for _, c := range e.containers {
		_ = testcontainers.TerminateContainer(c)
	}
}

func (e *Env) startPostgres(ctx context.Context) error {
	pg, err := tcpostgres.Run(ctx, PostgresImage,
		tcpostgres.WithDatabase("botsight"),
		tcpostgres.WithUsername("botsight"),
		tcpostgres.WithPassword("botsight"),
		tcpostgres.BasicWaitStrategies(),
	)
	e.track(pg)
	if err != nil {
		return fmt.Errorf("start postgres: %w", err)
	}
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return err
	}
	if e.Pool, err = pgxpool.New(ctx, dsn); err != nil {
		return err
	}
	return Migrate(ctx, e.Pool)
}

func (e *Env) startRedis(ctx context.Context) error {
	rc, err := tcredis.Run(ctx, RedisImage)
	e.track(rc)
	if err != nil {
		return fmt.Errorf("start redis: %w", err)
	}
	url, err := rc.ConnectionString(ctx)
	if err != nil {
		return err
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return err
	}
	e.Redis = redis.NewClient(opts)
	return nil
}

// startKafka runs the same apache/kafka image as docker compose in KRaft mode. The
// host port is known only after start, so the container waits for a file with the
// advertised listeners, written by a post-start hook.
func (e *Env) startKafka(ctx context.Context) error {
	const listenersFile = "/tmp/advertised_listeners"
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        KafkaImage,
			ExposedPorts: []string{"9094/tcp"},
			Env: map[string]string{
				"CLUSTER_ID":                                     "5L6g3nShT-eMCtK--X86sw",
				"KAFKA_NODE_ID":                                  "1",
				"KAFKA_PROCESS_ROLES":                            "broker,controller",
				"KAFKA_LISTENERS":                                "BROKER://:9092,CONTROLLER://:9093,EXTERNAL://:9094",
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT",
				"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
				"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
				"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
				"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
				"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
				"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
				"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			},
			Entrypoint: []string{"sh", "-c"},
			Cmd: []string{"while [ ! -s " + listenersFile + " ]; do sleep 0.1; done; " +
				"export KAFKA_ADVERTISED_LISTENERS=$(cat " + listenersFile + "); exec /etc/kafka/docker/run"},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{{
				PostStarts: []testcontainers.ContainerHook{func(ctx context.Context, c testcontainers.Container) error {
					host, err := c.Host(ctx)
					if err != nil {
						return err
					}
					port, err := c.MappedPort(ctx, "9094/tcp")
					if err != nil {
						return err
					}
					listeners := "BROKER://localhost:9092,EXTERNAL://" + net.JoinHostPort(host, port.Port())
					return c.CopyToContainer(ctx, []byte(listeners), listenersFile, 0o644)
				}},
			}},
			WaitingFor: wait.ForLog("Kafka Server started"),
		},
		Started: true,
	})
	e.track(c)
	if err != nil {
		return fmt.Errorf("start kafka: %w", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		return err
	}
	port, err := c.MappedPort(ctx, "9094/tcp")
	if err != nil {
		return err
	}
	e.KafkaBrokers = []string{net.JoinHostPort(host, port.Port())}
	return nil
}

func (e *Env) startClickHouse(ctx context.Context) error {
	c, err := tcclickhouse.Run(ctx, ClickHouseImage,
		tcclickhouse.WithDatabase("botsight"),
		tcclickhouse.WithUsername("botsight"),
		tcclickhouse.WithPassword("botsight"),
		// as in docker compose: migrations create a settings profile, ai-worker creates users
		testcontainers.WithEnv(map[string]string{"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"}),
	)
	e.track(c)
	if err != nil {
		return fmt.Errorf("start clickhouse: %w", err)
	}
	if e.ClickHouseAddr, err = c.ConnectionHost(ctx); err != nil {
		return err
	}
	e.ClickHouse, err = clickhouse.Open(ctx, clickhouse.Options{
		Addr: []string{e.ClickHouseAddr}, Database: "botsight", Username: "botsight", Password: "botsight",
	})
	if err != nil {
		return err
	}
	return MigrateClickHouse(ctx, e.ClickHouse)
}

func migrationsDir(kind string) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "migrations", kind)
}

// MigrationsDir is migrations/postgres at the repository root.
func MigrationsDir() string { return migrationsDir("postgres") }

func upFiles(kind string) ([]string, error) {
	dir := migrationsDir(kind)
	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no migrations in %s", dir)
	}
	sort.Strings(files)
	return files, nil
}

// Migrate applies every PostgreSQL *.up.sql in order, the same files the migrate container runs.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	files, err := upFiles("postgres")
	if err != nil {
		return err
	}
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", filepath.Base(f), err)
		}
	}
	return nil
}

// MigrateClickHouse applies every ClickHouse *.up.sql. The native protocol runs one
// statement per query, so files are split on semicolons (as golang-migrate does
// with x-multi-statement).
func MigrateClickHouse(ctx context.Context, conn driver.Conn) error {
	files, err := upFiles("clickhouse")
	if err != nil {
		return err
	}
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		for _, stmt := range strings.Split(string(sql), ";") {
			if strings.TrimSpace(stripComments(stmt)) == "" {
				continue
			}
			if err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("migration %s: %w", filepath.Base(f), err)
			}
		}
	}
	return nil
}

func stripComments(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
	return b.String()
}
