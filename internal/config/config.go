// Package config loads process configuration from CREP_* environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	MySQLDSN string

	CassandraHosts    []string
	CassandraKeyspace string
	CassandraRF       int
	CassandraUsername string
	CassandraPassword string

	HTTPAddr string // Task/Job API
	OpsAddr  string // /healthz, /readyz, /metrics

	EncryptionKeys string // kid:base64key[,kid:base64key...]

	JWTSecret        string
	JWTPublicKeyFile string
	JWTIssuer        string
	JWTAudience      string
	JWTTenantClaim   string

	AllowedPorts     string
	AllowPrivateDest bool
	DenyCIDRs        []string

	ExecutorID          string
	ExecutorConcurrency int
	ExecutorTenantLimit int
	LeaseGrace          time.Duration
	DrainTimeout        time.Duration

	SchedulerLookahead time.Duration
	ArchiveRetention   time.Duration // succeeded/cancelled jobs stay in MySQL this long
	ArchiveDeadAfter   time.Duration // dead jobs stay in the DLQ (retryable) this long
	HistoryTTL         time.Duration // Cassandra row TTL; 0 keeps history forever

	LogLevel string
}

func Load() (Config, error) {
	var errs []string
	str := func(k, def string) string {
		if v, ok := os.LookupEnv(k); ok {
			return v
		}
		return def
	}
	num := func(k string, def int) int {
		v := os.Getenv(k)
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, k+": not an integer")
		}
		return n
	}
	durv := func(k string, def time.Duration) time.Duration {
		v := os.Getenv(k)
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, k+": not a duration")
		}
		return d
	}
	list := func(k, def string) []string {
		var out []string
		for _, p := range strings.Split(str(k, def), ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
		return out
	}
	host, _ := os.Hostname()
	c := Config{
		MySQLDSN:            str("CREP_MYSQL_DSN", "crep:crep@tcp(127.0.0.1:3306)/crep"),
		CassandraHosts:      list("CREP_CASSANDRA_HOSTS", "127.0.0.1"),
		CassandraKeyspace:   str("CREP_CASSANDRA_KEYSPACE", "crep"),
		CassandraRF:         num("CREP_CASSANDRA_RF", 1),
		CassandraUsername:   str("CREP_CASSANDRA_USERNAME", ""),
		CassandraPassword:   str("CREP_CASSANDRA_PASSWORD", ""),
		HTTPAddr:            str("CREP_HTTP_ADDR", ":8080"),
		OpsAddr:             str("CREP_OPS_ADDR", ":9090"),
		EncryptionKeys:      str("CREP_ENCRYPTION_KEYS", ""),
		JWTSecret:           str("CREP_JWT_SECRET", ""),
		JWTPublicKeyFile:    str("CREP_JWT_PUBLIC_KEY_FILE", ""),
		JWTIssuer:           str("CREP_JWT_ISSUER", ""),
		JWTAudience:         str("CREP_JWT_AUDIENCE", ""),
		JWTTenantClaim:      str("CREP_JWT_TENANT_CLAIM", "tenant_id"),
		AllowedPorts:        str("CREP_ALLOWED_PORTS", "80,443"),
		AllowPrivateDest:    str("CREP_ALLOW_PRIVATE_DESTINATIONS", "false") == "true",
		DenyCIDRs:           list("CREP_DENY_CIDRS", ""),
		ExecutorID:          str("CREP_EXECUTOR_ID", host),
		ExecutorConcurrency: num("CREP_EXECUTOR_CONCURRENCY", 64),
		ExecutorTenantLimit: num("CREP_EXECUTOR_TENANT_LIMIT", 0),
		LeaseGrace:          durv("CREP_LEASE_GRACE", 30*time.Second),
		DrainTimeout:        durv("CREP_DRAIN_TIMEOUT", 30*time.Second),
		SchedulerLookahead:  durv("CREP_SCHEDULER_LOOKAHEAD", 30*time.Second),
		ArchiveRetention:    durv("CREP_ARCHIVE_RETENTION", time.Hour),
		ArchiveDeadAfter:    durv("CREP_ARCHIVE_DEAD_AFTER", 14*24*time.Hour),
		HistoryTTL:          durv("CREP_HISTORY_TTL", 30*24*time.Hour),
		LogLevel:            str("CREP_LOG_LEVEL", "info"),
	}
	if len(errs) > 0 {
		return c, fmt.Errorf("config: %s", strings.Join(errs, "; "))
	}
	return c, nil
}
