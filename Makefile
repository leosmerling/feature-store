SRC = $(wildcard **/src/*.py)

.PHONY: dev

dev:
	export FEATURE_STORE_APP_VERSION=0.1 && \
	export HOPEIT_ENGINE_VERSION=0.16.5 && \
	pip install -U pip && \
	pip install -U wheel && \
	pip install -U -r ops/requirements.txt
	pip install -U -r requirements-dev.txt
	cd apps/feature-store && \
	pip install -e .

start-app:
	FEATURE_STORE_APP_VERSION=0.1 \
	FEATURE_STORE_DATA_PATH=./_data \
	FEATURE_STORE_WORK_DIR=./_work \
	hopeit_server run --port=8020 --config-files=ops/server/server-config-local.json,ops/config-manager/plugin-config.json,apps/feature-store/config/feature-store.json

start-ops:
	FEATURE_STORE_APP_VERSION=0.1 \
	FEATURE_STORE_DATA_PATH=./_data \
	FEATURE_STORE_WORK_DIR=./_work \
	APPS_VISUALIZER_HOSTS=http://localhost:8020 \
	hopeit_server run --port=8040 --config-files=ops/server/server-config-local.json,ops/config-manager/plugin-config.json,ops/apps-visualizer/plugin-config.json
