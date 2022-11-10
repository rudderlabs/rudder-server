ifneq (,$(filter $(MAKECMDGOALS),deploy-throttling delete-throttling))
    ifeq ($(DOCKER_USER),)
        $(error DOCKER_USER is not set)
    endif
    ifeq ($(K8S_NAMESPACE),)
        $(error K8S_NAMESPACE is not set)
    endif
endif

ifeq ($(MAKECMDGOALS),build-throttling)
    ifeq ($(DOCKER_USER),)
        $(error DOCKER_USER is not set)
    endif
endif

ifneq (,$(filter $(MAKECMDGOALS),install-redis uninstall-redis))
    ifeq ($(K8S_NAMESPACE),)
        $(error K8S_NAMESPACE is not set)
    endif
endif

.PHONY: build-throttling
build-throttling:
	cd ../../; \
	docker build -t $(DOCKER_USER)/rudder-server-benchmarks:latest -f ./cmd/benchmark/Dockerfile .
	docker push $(DOCKER_USER)/rudder-server-benchmarks:latest

.PHONY: sed-throttling
sed-throttling:
	# e.g. make DOCKER_USER=fracasula K8S_NAMESPACE=fcasula deploy-throttling
	cp ./throttling/deployment.yaml /tmp/rudder-benchmarks-throttling-deployment.yaml
	sed -i "s/<DOCKER_USER>/$(DOCKER_USER)/g" /tmp/rudder-benchmarks-throttling-deployment.yaml
	sed -i "s/<K8S_NAMESPACE>/$(K8S_NAMESPACE)/g" /tmp/rudder-benchmarks-throttling-deployment.yaml

.PHONY: deploy-throttling
deploy-throttling: sed-throttling
	kubectl apply -f /tmp/rudder-benchmarks-throttling-deployment.yaml

.PHONY: delete-throttling
delete-throttling: sed-throttling
	# e.g. make DOCKER_USER=fracasula K8S_NAMESPACE=fcasula delete-throttling
	kubectl delete -f /tmp/rudder-benchmarks-throttling-deployment.yaml

.PHONY: install-redis
install-redis:
	helm install -n $(K8S_NAMESPACE) redis-throttling bitnami/redis \
		--set auth.enabled=false \
		--set replica.replicaCount=0

.PHONY: uninstall-redis
uninstall-redis:
	helm uninstall -n $(K8S_NAMESPACE) redis-throttling

.PHONY: local-redis
local-redis:
	docker run --rm -it -p 6379:6379 -e ALLOW_EMPTY_PASSWORD=yes bitnami/redis:6.2.7-debian-11-r56
