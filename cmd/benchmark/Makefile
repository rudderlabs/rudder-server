.PHONY: build-throttling
build-throttling:
	cd ../../; \
	docker build -t fracasula/rudder-server-benchmarks:latest -f Dockerfile-benchmarks .
	docker push fracasula/rudder-server-benchmarks:latest

.PHONY: deploy-throttling
deploy-throttling:
	kubectl apply -f ./throttling/deployment.yaml
