REQUIRED_VARS_BUILD = DOCKER_USER
REQUIRED_VARS_DEPLOY = DOCKER_USER K8S_NAMESPACE MODE

define check_vars
    $(foreach var,$(1),$(if $(value $(var)),,$(error $(var) is not set)))
endef

ifeq ($(MAKECMDGOALS),build-transformer)
    $(call check_vars,$(REQUIRED_VARS_BUILD))
endif

ifeq ($(MAKECMDGOALS),build-push-transformer)
    $(call check_vars,$(REQUIRED_VARS_BUILD))
endif

ifeq ($(MAKECMDGOALS),prepare-deployment)
    $(call check_vars,$(REQUIRED_VARS_DEPLOY))
endif

ifeq ($(MAKECMDGOALS),deploy-transformer)
    $(call check_vars,$(REQUIRED_VARS_DEPLOY))
endif

ifeq ($(MAKECMDGOALS),delete-transformer)
    $(call check_vars,$(REQUIRED_VARS_DEPLOY))
endif

IMAGE_NAME = $(DOCKER_USER)/rudder-warehouse-benchmarks:latest
DOCKERFILE_PATH = ./warehouse/transformer/benchmark/Dockerfile
DEPLOYMENT_TMP = /tmp/rudder-warehouse-benchmarks-transformer-deployment.yaml
PLATFORM = linux/arm64

.PHONY: all build-transformer build-push-transformer deploy-transformer delete-transformer

build-transformer:
	cd ../../../ && \
	docker buildx build -t $(IMAGE_NAME) -f $(DOCKERFILE_PATH) --platform=$(PLATFORM) .

build-push-transformer: build-transformer
	docker push $(IMAGE_NAME)

prepare-deployment:
	cp ./deployment.yaml $(DEPLOYMENT_TMP)
	sed -i'' -e "s|<DOCKER_USER>|$(DOCKER_USER)|g" $(DEPLOYMENT_TMP)
	sed -i'' -e "s|<K8S_NAMESPACE>|$(K8S_NAMESPACE)|g" $(DEPLOYMENT_TMP)
	sed -i'' -e "s|<MODE>|$(MODE)|g" $(DEPLOYMENT_TMP)

deploy-transformer: prepare-deployment
	kubectl apply -f $(DEPLOYMENT_TMP)

delete-transformer: prepare-deployment
	kubectl delete -f $(DEPLOYMENT_TMP)
