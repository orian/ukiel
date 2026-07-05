# Ukiel developer targets.

.PHONY: test e2e e2e-up e2e-down

# Unit + component tests (Docker required for testcontainers).
test:
	cargo test

# End-to-end suite against the docker-compose stack.
# Brings the stack up, runs the ignored e2e scenarios single-threaded, tears down.
e2e:
	docker compose up -d --wait
	cargo test -p ukiel-e2e -- --ignored --test-threads=1
	docker compose down -v

e2e-up:
	docker compose up -d --wait

e2e-down:
	docker compose down -v
