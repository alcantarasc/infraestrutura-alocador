.PHONY: start-minikube deploy-elk forward-kibana clean

# Start Minikube with 6GB RAM
start-minikube:
	minikube start --memory=6144 --cpus=2

# Deploy ELK stack
deploy-elk:
	kubectl apply -f elk-namespace.yml
	kubectl apply -f elasticsearch.yml
	kubectl apply -f kibana-service-account.yml
	kubectl create token kibana -n elk > kibana-token
	kubectl create secret generic kibana-service-account-token --from-literal=token=$$(cat kibana-token) -n elk
	rm kibana-token
	kubectl apply -f kibana.yml
	kubectl apply -f logstash.yml
	@echo "Waiting for Elasticsearch to be ready..."
	kubectl wait --namespace elk --for=condition=ready pod -l app=elasticsearch --timeout=300s
	@echo "Waiting for Kibana to be ready..."
	kubectl wait --namespace elk --for=condition=ready pod -l app=kibana --timeout=300s
	@echo "Waiting for Logstash to be ready..."
	kubectl wait --namespace elk --for=condition=ready pod -l app=logstash --timeout=300s

# Forward Kibana port
forward-kibana:
	@echo "Forwarding Kibana port 5601..."
	kubectl port-forward --address 0.0.0.0 -n elk svc/kibana 5601:5601

# Clean up resources
clean:
	kubectl delete -f logstash.yml
	kubectl delete -f kibana.yml
	kubectl delete -f elasticsearch.yml
	kubectl delete -f elk-namespace.yml
	minikube delete

# Show status of pods
status:
	kubectl get pods -n elk

# Show logs for all components
logs:
	@echo "=== Elasticsearch Logs ==="
	kubectl logs -n elk -l app=elasticsearch
	@echo "\n=== Kibana Logs ==="
	kubectl logs -n elk -l app=kibana
	@echo "\n=== Logstash Logs ==="
	kubectl logs -n elk -l app=logstash

# Help command
help:
	@echo "Available commands:"
	@echo "  make start-minikube    - Start Minikube with 6GB RAM"
	@echo "  make deploy-elk        - Deploy ELK stack"
	@echo "  make forward-kibana    - Forward Kibana port 5601"
	@echo "  make clean            - Clean up all resources"
	@echo "  make status           - Show status of pods"
	@echo "  make logs             - Show logs for all components" 