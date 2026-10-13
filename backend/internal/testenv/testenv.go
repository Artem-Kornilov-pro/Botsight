// Package testenv starts PostgreSQL and Redis in containers for integration tests
// and applies the migrations from migrations/postgres.
package testenv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// Images match deploy/docker-compose.yml.
const (
	PostgresImage = "postgres:17-alpine"
	RedisImage    = "redis:7.4-alpine"
)

type Env struct {
	Pool  *pgxpool.Pool
	Redis *redis.Client

	containers []testcontainers.Container
}

func Start(ctx context.Context) (*Env, error) {
	env := &Env{}
	pg, err := tcpostgres.Run(ctx, PostgresImage,
		tcpostgres.WithDatabase("botsight"),
		tcpostgres.WithUsername("botsight"),
		tcpostgres.WithPassword("botsight"),
		tcpostgres.BasicWaitStrategies(),
	)
	if pg != nil {
		env.containers = append(env.containers, pg)
	}
	if err != nil {
		env.Close()
		return nil, fmt.Errorf("start postgres: %w", err)
	}
	rc, err := tcredis.Run(ctx, RedisImage)
	if rc != nil {
		env.containers = append(env.containers, rc)
	}
	if err != nil {
		env.Close()
		return nil, fmt.Errorf("start redis: %w", err)
	}

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		env.Close()
		return nil, err
	}
	if env.Pool, err = pgxpool.New(ctx, dsn); err != nil {
		env.Close()
		return nil, err
	}
	if err := Migrate(ctx, env.Pool); err != nil {
		env.Close()
		return nil, err
	}

	redisURL, err := rc.ConnectionString(ctx)
	if err != nil {
		env.Close()
		return nil, err
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		env.Close()
		return nil, err
	}
	env.Redis = redis.NewClient(opts)
	return env, nil
}

func (e *Env) Close() {
	if e.Pool != nil {
		e.Pool.Close()
	}
	if e.Redis != nil {
		_ = e.Redis.Close()
	}
	for _, c := range e.containers {
		_ = testcontainers.TerminateContainer(c)
	}
}

// MigrationsDir is migrations/postgres at the repository root.
func MigrationsDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "migrations", "postgres")
}

// Migrate applies every *.up.sql in order, the same files the migrate container runs.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	dir := MigrationsDir()
	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no migrations in %s", dir)
	}
	sort.Strings(files)
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", strings.TrimSuffix(filepath.Base(f), ".up.sql"), err)
		}
	}
	return nil
}
