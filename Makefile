KT := docker compose exec kafka1 /opt/kafka/bin

.PHONY: up down clean ps logs lag lake topics load scale \
        chaos-broker heal-broker chaos-consumer heal-consumer chaos-schema heal-schema

up:            ## Build and start the whole stack
	docker compose up -d --build

down:          ## Stop the stack, keep data
	docker compose down

clean:         ## Stop the stack and delete all Kafka/Prometheus/Grafana data
	docker compose down -v

ps:            ## Show service status
	docker compose ps

logs:          ## Follow pipeline logs
	docker compose logs -f producer etl-consumer

topics:        ## Describe the topics
	$(KT)/kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic plant.events

lag:           ## Show consumer-group lag per partition
	$(KT)/kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --describe --group plant-etl

lake:          ## List the latest Parquet files in the data lake
	docker compose exec localstack awslocal s3 ls s3://plant-lake/events/ --recursive --human-readable 2>/dev/null | grep parquet | tail -10

load:          ## Raise load: make load RATE=20000
	EVENTS_PER_SEC=$(or $(RATE),20000) docker compose up -d --no-deps producer

scale:         ## Scale the pipeline: make scale P=3 C=6
	docker compose up -d --no-deps --scale producer=$(or $(P),2) --scale etl-consumer=$(or $(C),3) producer etl-consumer

chaos-broker:  ## Kill one broker (replication / ISR shrink)
	docker compose stop kafka2
heal-broker:
	docker compose start kafka2

chaos-consumer: ## Stop the ETL (freshness + consumer lag)
	docker compose stop etl-consumer
heal-consumer:
	docker compose up -d --no-deps etl-consumer

chaos-schema:  ## 5% of records lose a field (schema + DLQ)
	SCHEMA_BREAK_RATE=0.05 docker compose up -d --no-deps producer
heal-schema:
	SCHEMA_BREAK_RATE=0.001 docker compose up -d --no-deps producer

help:          ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-15s %s\n", $$1, $$2}'
