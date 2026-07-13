# Ukiel developer targets.

.PHONY: test e2e e2e-ha e2e-up e2e-down play bench-fetch-hits bench-fetch-bluesky bench-report

# Unit + component tests (Docker required for testcontainers).
# --test-threads=2: each component test starts its own containers; unbounded
# parallelism (one container per CPU) makes testcontainers' log-wait flake.
test:
	cargo test -- --test-threads=2

# End-to-end suite against the docker-compose stack.
# Brings the stack up, runs the ignored e2e scenarios single-threaded, tears down.
e2e:
	docker compose up -d --wait
	cargo test -p ukiel-e2e -- --ignored --test-threads=1
	docker compose down -v

# HA catalog-outage suite (plan 42, S10). Adds the opt-in Toxiproxy profile,
# runs ONLY the S10 scenario serially, and always tears the stack down — the
# trap matters: a failed run must not leave a toxic proxy in front of Postgres
# for the next `make e2e`.
e2e-ha:
	docker compose --profile ha up -d --wait
	trap 'docker compose --profile ha down -v' EXIT; \
	  UKIEL_E2E_HA=1 cargo test -p ukiel-e2e --test s10_catalog_recovery -- --ignored --test-threads=1

e2e-up:
	docker compose up -d --wait

e2e-down:
	docker compose down -v

# Macro perf datasets (plan 30; see docs/notes/2026-07-08-macro-perf.md).
# ClickBench hits, partitioned: ~15 GB total, 100 files. `-C -` resumes.
bench-fetch-hits:
	mkdir -p bench/datasets/hits
	for i in $$(seq 0 99); do \
	  curl -f -C - -o bench/datasets/hits/hits_$$i.parquet \
	    https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_$$i.parquet; \
	done

# Bluesky (JSONBench): FILES=100 is the 100M tier (~12.5 GB), FILES=1000 the 1B tier.
bench-fetch-bluesky:
	mkdir -p bench/datasets/bluesky
	for i in $$(seq -f '%04g' 1 $(or $(FILES),100)); do \
	  curl -f -C - -o bench/datasets/bluesky/file_$$i.json.gz \
	    https://clickhouse-public-datasets.s3.amazonaws.com/bluesky/file_$$i.json.gz; \
	done

# Regenerate the self-contained HTML results page from bench/results/*.json.
bench-report:
	python3 bench/report.py html
	@echo "open bench/results/report.html"

# Run the all-in-one server against the compose stack with the example config.
play:
	docker compose up -d --wait
	cargo run -p ukield -- --config ukield.example.toml
