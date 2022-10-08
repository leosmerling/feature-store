SRC = $(wildcard **/src/*.py)

.PHONY: dev

dev:
	export FEATURE_STORE_APP_VERSION=0.1 && \
	export HOPEIT_ENGINE_VERSION=0.16.5 && \
	mkdir -p _data && \
	mkdir -p _work && \
	pip install -U pip && \
	pip install -U wheel && \
	pip install -U -r ops/requirements.txt
	pip install -U -r requirements-dev.txt
	cd apps/feature-store && \
	pip install -e .

update-api:
	FEATURE_STORE_APP_VERSION=0.1 \
	FEATURE_STORE_DATA_PATH=./_data \
	FEATURE_STORE_WORK_DIR=./_work \
	hopeit_openapi create \
		--title="Feature Store" \
		--description="https://github.com/leosmerling/feature-store" \
		--api-version="v0" \
		--config-files=ops/server/server-config-local.json,ops/config-manager/plugin-config.json,apps/feature-store/config/feature-store.json \
		--output-file=apps/feature-store/config/openapi.json

start-app:
	docker compose up -d redis && \
	FEATURE_STORE_APP_VERSION=0.1 \
	FEATURE_STORE_DATA_PATH=./_data \
	FEATURE_STORE_WORK_DIR=./_work \
	hopeit_server run \
		--port=8020 \
		--start-streams \
		--config-files=ops/server/server-config-local.json,ops/config-manager/plugin-config.json,apps/feature-store/config/feature-store.json \
		--api-file=apps/feature-store/config/openapi.json

start-ops:
	FEATURE_STORE_APP_VERSION=0.1 \
	FEATURE_STORE_DATA_PATH=./_data \
	FEATURE_STORE_WORK_DIR=./_work \
	APPS_VISUALIZER_HOSTS=http://localhost:8020 \
	hopeit_server run --port=8040 --config-files=ops/server/server-config-local.json,ops/config-manager/plugin-config.json,ops/apps-visualizer/plugin-config.json
