CXX      ?= g++
CXXFLAGS ?= -O3 -march=x86-64-v3 -mtune=cascadelake -std=c++17 -pthread
PY       ?= .venv/bin/python

.PHONY: build fixtures check-dev bench docker

pipeline: src/main.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

build: pipeline

## all 36 edge-case fixtures
fixtures: pipeline
	$(PY) scripts/run_fixtures.py fixtures -- ./pipeline

## 1M-event dev set against the published answer (data/dev, vendor/dev-answer)
check-dev: pipeline
	rm -rf out && mkdir out && DATA_DIR=data/dev OUTPUT_DIR=out ./pipeline
	cd scripts && ../$(PY) compare.py ../out ../vendor/dev-answer/dev-answer

## 20M-event practice-size set, 8 CPUs, phase timings on stderr
bench: pipeline
	rm -rf out && mkdir out && taskset -c 0-7 env DATA_DIR=data/p20 OUTPUT_DIR=out NCPU=8 PIPE_DEBUG=1 ./pipeline

docker:
	docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
