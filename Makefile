.PHONY: test run-exchange run-stocky watch
test:
	go vet ./... && go test -race -count=1 ./...
run-exchange:
	go run ./cmd/exchange
run-stocky:
	go run ./cmd/stocky
watch:
	go run ./cmd/watch -user u1
