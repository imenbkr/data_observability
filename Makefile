.PHONY: up down clean ps logs flights topic routes notebook help

up:       ## Build and start the stack
	docker compose up -d --build

down:     ## Stop the stack, keep data
	docker compose down

clean:    ## Stop the stack and delete all data
	docker compose down -v

ps:       ## Show service status
	docker compose ps

logs:     ## Follow producer and consumer logs
	docker compose logs -f producer consumer

flights:  ## Show the Tunisia flights found in the latest polls
	docker compose logs producer | grep 'poll:' | tail -5

topic:    ## Read the 3 latest messages from Kafka
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
	  --topic tunisia.flights --max-messages 3 --from-beginning --timeout-ms 10000

routes:   ## Refresh data/routes.csv from public sources, then restart the producer
	python3 scripts/build_routes.py && docker compose restart producer

help:     ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

notebook: ## Build the "Detect data volume drops" notebook and deploy it to local Grafana
	python3 scripts/build_notebook.py && python3 scripts/deploy_notebook.py
