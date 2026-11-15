.PHONY: build pack
build:
	mkdir -p bin
	go build -trimpath -o bin/notifications-worker ./cmd/notifications-worker

pack:
	bash scripts/pack.sh dist
