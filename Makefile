UV ?= uv
COV_MIN ?= 85

.PHONY: check fmt fmt-check lint typecheck test cov demo demo-ollama bench golden clean

check: fmt-check lint typecheck test

fmt:
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

fmt-check:
	$(UV) run ruff format --check .

lint:
	$(UV) run ruff check .

typecheck:
	$(UV) run mypy

test:
	$(UV) run pytest --cov=replayagent --cov-report=term-missing:skip-covered --cov-fail-under=$(COV_MIN)

demo:
	$(UV) run python -u scripts/demo.py

demo-ollama:
	$(UV) run python -u scripts/demo_ollama.py

bench:
	$(UV) run python -u scripts/bench.py

GOLDEN ?= research_v1

golden:
	$(UV) run python -u scripts/make_golden.py $(GOLDEN)

clean:
	rm -rf .demo .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
