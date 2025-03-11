.PHONY: run execute clean

run:
	@echo "Build & Run..."
	@mvn install -Dnative
	docker compose up -d
	$(MAKE) wait

execute:
	./target/debezium-native-1.0-SNAPSHOT-runner

wait:
	@echo "Waiting for services to be healthy..."
	@while ! docker compose ps | grep '(database system is ready to accept connections)'; do sleep 2; done
	@echo "All services are up!"

clean:
	@echo "Cleaning up..."
	@mvn clean
	@docker compose down