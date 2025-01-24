PLUGIN_NAME ?= proxy-cache-s3
BUILDER_REGISTRY ?= higress-registry.cn-hangzhou.cr.aliyuncs.com/plugins/
REGISTRY ?= higress-registry.cn-hangzhou.cr.aliyuncs.com/plugins/
GO_VERSION ?= 1.20.14
TINYGO_VERSION ?= 0.29.0
ORAS_VERSION ?= 1.0.0
HIGRESS_VERSION ?= 1.0.0-rc
USE_HIGRESS_TINYGO ?= false
BUILDER ?= ${BUILDER_REGISTRY}wasm-go-builder:go${GO_VERSION}-tinygo${TINYGO_VERSION}-oras${ORAS_VERSION}
BUILD_TIME := $(shell date "+%Y%m%d-%H%M%S")
COMMIT_ID := $(shell git rev-parse --short HEAD 2>/dev/null)
IMAGE_TAG = $(if $(strip $(PLUGIN_VERSION)),${PLUGIN_VERSION},${BUILD_TIME}-${COMMIT_ID})
IMG ?= ${REGISTRY}${PLUGIN_NAME}:${IMAGE_TAG}
GOPROXY := $(shell go env GOPROXY)
EXTRA_TAGS ?= proxy_wasm_version_0_2_100

IMAGE_VERSION ?= 1.0.0

.DEFAULT:
build:
	docker compose down
	DOCKER_BUILDKIT=1 docker build --build-arg PLUGIN_NAME=${PLUGIN_NAME} \
	                            --build-arg BUILDER=${BUILDER}  \
	                            --build-arg GOPROXY=$(GOPROXY) \
	                            --build-arg EXTRA_TAGS=$(EXTRA_TAGS) \
	                            -t ${IMG} \
	                            --output extensions/${PLUGIN_NAME} \
	                            .
	@echo ""
	@echo "output wasm file: extensions/${PLUGIN_NAME}/plugin.wasm"
	docker compose up -d --build
	docker build -t docker.wos.w7.com/public/proxy:go-${IMAGE_VERSION} -f Dockerfile-proxy .
	docker push docker.wos.w7.com/public/proxy:go-${IMAGE_VERSION}

build-image:
	DOCKER_BUILDKIT=1 docker build --build-arg PLUGIN_NAME=${PLUGIN_NAME} \
	                            --build-arg BUILDER=${BUILDER}  \
	                            --build-arg GOPROXY=$(GOPROXY) \
	                            --build-arg EXTRA_TAGS=$(EXTRA_TAGS) \
	                            -t ${IMG} \
	                            .
	@echo ""
	@echo "image:            ${IMG}"

build-push: build-image
	docker push ${IMG}

# builder:
# To build a wasm-go-builder image.
# e.g.
#   REGISTRY=<your_docker_registry> make builder
# If you want to use Go/TinyGo/Oras with another version, please modify GO_VERSION/TINYGO_VERSION/ORAS_VERSION.
# After your wasm-go-builder image is built, you can use it to build plugin image.
# e.g.
#   PLUGIN_NAME=request-block BUILDER=<your-wasm-go-builder> make
builder:
	docker buildx build --no-cache \
			--platform linux/amd64,linux/arm64 \
			--build-arg BASE_IMAGE=docker.io/ubuntu \
			--build-arg GO_VERSION=$(GO_VERSION) \
			--build-arg TINYGO_VERSION=$(TINYGO_VERSION) \
			--build-arg ORAS_VERSION=$(ORAS_VERSION) \
			--build-arg HIGRESS_VERSION=$(HIGRESS_VERSION) \
			--build-arg USE_HIGRESS_TINYGO=$(USE_HIGRESS_TINYGO) \
			-f DockerfileBuilder \
			-t ${BUILDER} \
			--push \
			.
	@echo ""
	@echo "image: ${BUILDER}"

local-build:
	tinygo build -scheduler=none -target=wasi -gc=custom -tags='custommalloc nottinygc_finalizer' \
		-o extensions/${PLUGIN_NAME}/main.wasm \
		extensions/${PLUGIN_NAME}/main.go
	@echo ""
	@echo "wasm: extensions/${PLUGIN_NAME}/main.wasm"

local-run:
	python3 .devcontainer/gen_config.py ${PLUGIN_NAME}
	envoy -c extensions/${PLUGIN_NAME}/config.yaml --concurrency 0 --log-level info --component-log-level wasm:debug

local-all: local-build local-run