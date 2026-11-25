PY ?= .venv/bin/python

.PHONY: venv test test-fast test-docker up down logs
venv:
	uv venv -q -p 3.12 .venv && uv pip install -q -p .venv pytest
test:            ## everything, including the docker tests (project hwlock-test, random port)
	env -u PYTHONPATH $(PY) -m pytest -q
test-fast:       ## no docker
	env -u PYTHONPATH $(PY) -m pytest -q -m "not docker"
test-docker:
	env -u PYTHONPATH $(PY) -m pytest -q -m docker
up:              ## deploy / update the service on this machine
	docker compose up -d --build
down:
	docker compose down
logs:
	docker compose logs -f --tail=100
