SHELL := /bin/bash

.DEFAULT_GOAL := help

.PHONY: help

ENVIRONMENT_VARIABLES_FILE := .env

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

setup: ## Setup environment for local development
	docker compose -f tools/docker-compose.yaml up -d --remove-orphans

destroy: ## Destroy environment for local development
	docker compose -f tools/docker-compose.yaml down -v -t 0 --remove-orphans
