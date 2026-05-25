.PHONY: build test integration-test load-test start stop clean release

build:
	go build -o aquifer .

test: build start-servers
	@echo "Running smoke tests..."
	@hurl --test tests/smoke.hurl || (make stop-servers; exit 1)
	@make stop-servers
	@echo "Tests passed!"

integration-test: build start-servers
	@echo "Running smoke + load tests..."
	@hurl --test tests/smoke.hurl || (make stop-servers; exit 1)
	@hurl --test --repeat 50 --jobs 10 tests/load.hurl || (make stop-servers; exit 1)
	@echo ""
	@echo "Running SSE streaming tests..."
	@AQUIFER_URL=http://localhost:8080 TARGET_URL=http://localhost:9000 \
		python3 tests/test_stream.py || (make stop-servers; exit 1)
	@echo ""
	@echo "Running L8 protocol tests..."
	@AQUIFER_URL=http://localhost:8080 TARGET_URL=http://localhost:9000 RECEIVER_URL=http://localhost:9001 \
		python3 tests/test_l8.py || (make stop-servers; exit 1)
	@make stop-servers
	@echo ""
	@echo "All integration tests passed!"

start-servers:
	@echo "Starting target server on :9000..."
	@python3 tests/target_server.py > /tmp/aquifer_target.log 2>&1 & echo $$! > /tmp/aquifer_target.pid
	@sleep 0.3
	@echo "Starting L8 receiver on :9001..."
	@python3 tests/l8_receiver.py > /tmp/aquifer_l8_receiver.log 2>&1 & echo $$! > /tmp/aquifer_l8_receiver.pid
	@sleep 0.3
	@echo "Starting Aquifer on :8080..."
	@DB_PATH=/tmp/aquifer_test.db ./aquifer > /tmp/aquifer.log 2>&1 & echo $$! > /tmp/aquifer.pid
	@sleep 0.5

stop-servers:
	@[ -f /tmp/aquifer.pid ] && kill $$(cat /tmp/aquifer.pid) 2>/dev/null || true; rm -f /tmp/aquifer.pid
	@[ -f /tmp/aquifer_target.pid ] && kill $$(cat /tmp/aquifer_target.pid) 2>/dev/null || true; rm -f /tmp/aquifer_target.pid
	@[ -f /tmp/aquifer_l8_receiver.pid ] && kill $$(cat /tmp/aquifer_l8_receiver.pid) 2>/dev/null || true; rm -f /tmp/aquifer_l8_receiver.pid
	@rm -f /tmp/aquifer_test.db

clean:
	@rm -f aquifer aquifer.db *.db-shm *.db-wal

release:
	@if [ -z "$(VERSION)" ]; then echo "Usage: make release VERSION=0.1.0"; exit 1; fi
	@echo "Releasing v$(VERSION)..."
	@git tag -a v$(VERSION) -m "Release v$(VERSION)"
	@git push origin v$(VERSION)
	@gh release create v$(VERSION) --title "v$(VERSION)" --generate-notes
	@echo "Released v$(VERSION)!"
