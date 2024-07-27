.PHONY: run

dev: 
	air

test:
	go test -v ./... -race