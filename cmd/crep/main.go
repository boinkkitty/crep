// Command crep runs one Crep service per process, and is also the CLI client for its APIs:
//
//	crep serve <task-api|job-api|scheduler|executor|reconciler|archiver>
//	crep keygen
//	crep token <tenant> [-ttl 24h]
//	crep task ... | crep job ...   (see cli.go)
//	crep keygen
//	crep token --tenant=acme --ttl=24h   (dev: mint an HS256 token with CREP_JWT_SECRET)
package main

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"crep/internal/adapters/cassandra"
	"crep/internal/adapters/clock"
	"crep/internal/adapters/crypto"
	"crep/internal/adapters/httpapi"
	"crep/internal/adapters/httpcaller"
	"crep/internal/adapters/idgen"
	"crep/internal/adapters/metrics"
	"crep/internal/adapters/mysql"
	"crep/internal/app/services"
	"crep/internal/config"
)

var serviceNames = []string{"task-api", "job-api", "scheduler", "executor", "reconciler", "archiver"}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "keygen":
		k := make([]byte, 32)
		_, _ = rand.Read(k)
		fmt.Printf("k%s:%s\n", time.Now().UTC().Format("20060102"), base64.StdEncoding.EncodeToString(k))
	case "token":
		err = mintToken(os.Args[2:])
	case "task", "job":
		if len(os.Args) < 3 {
			usage()
		}
		if os.Args[1] == "task" {
			err = taskCmd(os.Args[2], os.Args[3:])
		} else {
			err = jobCmd(os.Args[2], os.Args[3:])
		}
	default:
		usage()
	}
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "crep:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "crep: distributed cron.\n\nServices (configured by CREP_* env):\n  crep serve %s\n  crep keygen                           print a new encryption key entry\n\n%s",
		strings.Join(serviceNames, "|"), cliUsage)
	os.Exit(2)
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	_ = l.UnmarshalText([]byte(level))
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

func serve(args []string) error {
	if len(args) != 1 || !slices.Contains(serviceNames, args[0]) {
		return fmt.Errorf("serve takes exactly one service: %s", strings.Join(serviceNames, "|"))
	}
	service := args[0]
	has := func(m ...string) bool { return slices.Contains(m, service) }

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel).With("service", service)

	keys, err := crypto.ParseKeyring(cfg.EncryptionKeys)
	if err != nil {
		return fmt.Errorf("CREP_ENCRYPTION_KEYS: %w", err)
	}
	db, err := mysql.Open(cfg.MySQLDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	clk := clock.System{}
	store := mysql.NewStore(db, crypto.RequestCodec{K: keys}, clk)

	policy, err := destinationPolicy(cfg)
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	prom := metrics.NewProm(reg)
	jobRepo := store.JobRepo()
	if has("reconciler") {
		metrics.DLQGauge(reg, func() (int64, error) { return jobRepo.CountDead(context.Background()) })
	}

	deps := services.Deps{Tx: store, Tasks: store.TaskRepo(), Jobs: jobRepo, Dest: policy,
		Clock: clk, IDs: idgen.UUIDv7{}, Metrics: prom, Log: log}

	var hist *cassandra.Store
	if has("job-api", "archiver") {
		hist, err = cassandra.Open(cassandra.Config{Hosts: cfg.CassandraHosts, Keyspace: cfg.CassandraKeyspace,
			ReplicationFactor: cfg.CassandraRF, TTL: cfg.HistoryTTL, Username: cfg.CassandraUsername, Password: cfg.CassandraPassword})
		if err != nil {
			return fmt.Errorf("cassandra: %w", err)
		}
		defer hist.Close()
		deps.History = hist
	}
	if has("executor") {
		deps.Caller = httpcaller.New(policy)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var wg sync.WaitGroup
	run := func(name string, fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("starting", "service", name)
			fn(ctx)
			log.Info("stopped", "service", name)
		}()
	}

	if has("scheduler") {
		s := services.NewScheduler(deps, services.SchedulerConfig{Lookahead: cfg.SchedulerLookahead})
		run("scheduler", s.Run)
	}
	if has("executor") {
		id := fmt.Sprintf("%s-%d-%s", cfg.ExecutorID, os.Getpid(), idgen.UUIDv7{}.NewID()[24:])
		e := services.NewExecutor(deps, services.ExecutorConfig{ID: id, Concurrency: cfg.ExecutorConcurrency,
			PerTenantLimit: cfg.ExecutorTenantLimit, LeaseGrace: cfg.LeaseGrace, DrainTimeout: cfg.DrainTimeout})
		run("executor", e.Run)
	}
	if has("reconciler") {
		run("reconciler", services.NewReconciler(deps, services.ReconcilerConfig{}).Run)
	}
	if has("archiver") {
		run("archiver", services.NewArchiver(deps, services.ArchiverConfig{Retention: cfg.ArchiveRetention, DeadRetention: cfg.ArchiveDeadAfter}).Run)
	}
	if has("task-api", "job-api") {
		auth, err := authenticator(cfg)
		if err != nil {
			return err
		}
		api := &httpapi.API{Auth: auth, Log: log}
		if has("task-api") {
			api.Tasks = services.NewTaskService(deps)
		}
		if has("job-api") {
			api.Jobs = services.NewJobService(deps)
		}
		mux := http.NewServeMux()
		mux.Handle("/v1/", api.Handler())
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
		run("http", httpServer(cfg.HTTPAddr, mux, log))
	}

	ops := http.NewServeMux()
	ops.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	ops.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	ops.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := store.Ping(ctx); err != nil {
			http.Error(w, "mysql: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		if hist != nil {
			if err := hist.Ping(ctx); err != nil {
				http.Error(w, "cassandra: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = w.Write([]byte("ready"))
	})
	run("ops", httpServer(cfg.OpsAddr, ops, log))

	<-ctx.Done()
	log.Info("shutting down")
	wg.Wait()
	return nil
}

func httpServer(addr string, h http.Handler, log *slog.Logger) func(context.Context) {
	return func(ctx context.Context) {
		srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second}
		go func() {
			<-ctx.Done()
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
		}()
		log.Info("listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "addr", addr, "err", err)
		}
	}
}

func destinationPolicy(cfg config.Config) (httpcaller.Policy, error) {
	ports, err := httpcaller.ParsePorts(cfg.AllowedPorts)
	if err != nil {
		return httpcaller.Policy{}, fmt.Errorf("CREP_ALLOWED_PORTS: %w", err)
	}
	p := httpcaller.Policy{AllowedPorts: ports, AllowPrivate: cfg.AllowPrivateDest}
	for _, c := range cfg.DenyCIDRs {
		pre, err := netip.ParsePrefix(c)
		if err != nil {
			return p, fmt.Errorf("CREP_DENY_CIDRS: %w", err)
		}
		p.DenyCIDRs = append(p.DenyCIDRs, pre)
	}
	return p, nil
}

func authenticator(cfg config.Config) (httpapi.Authenticator, error) {
	jc := httpapi.JWTConfig{HMACSecret: []byte(cfg.JWTSecret), Issuer: cfg.JWTIssuer,
		Audience: cfg.JWTAudience, Claim: cfg.JWTTenantClaim}
	if cfg.JWTPublicKeyFile != "" {
		raw, err := os.ReadFile(cfg.JWTPublicKeyFile)
		if err != nil {
			return nil, err
		}
		block, _ := pem.Decode(raw)
		if block == nil {
			return nil, errors.New("CREP_JWT_PUBLIC_KEY_FILE: no PEM block")
		}
		if jc.PublicKey, err = x509.ParsePKIXPublicKey(block.Bytes); err != nil {
			return nil, err
		}
	}
	return httpapi.NewJWTAuth(jc)
}

func mintToken(args []string) error {
	tenant, rest := splitID(args)
	fs := flag.NewFlagSet("token", flag.ContinueOnError)
	ttl := fs.Duration("ttl", 24*time.Hour, "token lifetime")
	if err := parseFlags(fs, rest); err != nil {
		return err
	}
	if tenant == "" {
		return errors.New("usage: crep token <tenant> [-ttl 24h]")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if len(cfg.JWTSecret) < 32 {
		return errors.New("CREP_JWT_SECRET (>= 32 bytes) is required")
	}
	claims := jwt.MapClaims{cfg.JWTTenantClaim: tenant, "exp": time.Now().Add(*ttl).Unix(), "iat": time.Now().Unix()}
	if cfg.JWTIssuer != "" {
		claims["iss"] = cfg.JWTIssuer
	}
	if cfg.JWTAudience != "" {
		claims["aud"] = cfg.JWTAudience
	}
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(cfg.JWTSecret))
	if err != nil {
		return err
	}
	fmt.Println(s)
	return nil
}
