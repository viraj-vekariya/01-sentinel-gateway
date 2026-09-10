# Sentinel Gateway
#
#   make setup   install deps and build the native extension
#   make test    run the suite
#   make run     gateway + both upstreams locally
#   make bench   the GIL benchmark (writes outputs/bench_ratelimit.json)
#   make demo    drive traffic at a running gateway (writes outputs/e2e_results.json)
#   make docker  full stack in containers

PY      ?= python3
JAVA_HOME ?= $(shell /usr/libexec/java_home -v 21 2>/dev/null || echo /opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home)
MVN      = JAVA_HOME=$(JAVA_HOME) mvn

.PHONY: setup native test bench demo run run-gateway run-catalog run-pricing \
        docker clean results

setup: native
	$(PY) -m pip install -r requirements.txt

native:
	$(PY) native/setup.py build_ext --inplace
	@$(PY) -c "import sentinel_native; print('native ok', sentinel_native.__version__)"

test:
	$(PY) -m pytest tests/ -q

bench:
	$(PY) bench/bench_ratelimit.py

demo:
	$(PY) bench/demo_traffic.py

run-catalog:
	$(PY) services/python-service/app.py

run-pricing:
	cd services/java-service && $(MVN) -q spring-boot:run

run-gateway:
	$(PY) -m uvicorn gateway.main:app --host 127.0.0.1 --port 8000

# Everything in one terminal. `wait` keeps the shell alive; the trap makes Ctrl-C
# kill the children too rather than orphaning three servers.
run:
	@echo "starting catalog(8101) pricing(8102) gateway(8000) - Ctrl-C stops all"
	@trap 'kill 0' EXIT INT TERM; \
	$(PY) services/python-service/app.py & \
	(cd services/java-service && $(MVN) -q spring-boot:run) & \
	sleep 12; \
	$(PY) -m uvicorn gateway.main:app --host 127.0.0.1 --port 8000 & \
	wait

docker:
	docker compose up --build

results: bench
	@echo "run 'make run' in another terminal, then 'make demo'"

clean:
	rm -rf build/ *.so native/build/ .pytest_cache __pycache__ \
	       gateway/__pycache__ gateway/*/__pycache__ tests/__pycache__ \
	       services/java-service/target outputs/sentinel.db*
