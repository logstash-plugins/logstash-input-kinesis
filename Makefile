.PHONY: help test clean dist-clean setup all integration integration-clean gem docker

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

vendor/bundle/.timestamp: Gemfile
	@echo "Installing Ruby dependencies..."
	bundle install --with development
	@mkdir -p vendor/bundle
	@touch vendor/bundle/.timestamp

install: vendor/bundle/.timestamp ## Install Ruby dependencies (including development gems)

vendor/jar-dependencies/.timestamp: vendor/bundle/.timestamp logstash-input-kinesis.gemspec
	@echo "Downloading Java JAR dependencies..."
	bundle exec rake install_jars
	@touch vendor/jar-dependencies/.timestamp

install-jars: vendor/jar-dependencies/.timestamp ## Download Java JAR dependencies

test: install-jars ## Run unit tests
	@echo "Running tests..."
	bundle exec rspec

gem: install-jars ## Build gem package
	@echo "Building gem package..."
	gem build logstash-input-kinesis.gemspec

docker: ## Build Docker image with the plugin installed
	@echo "Building Docker image..."
	docker build -t logstash-input-kinesis .

integration: ## Run integration tests with docker-compose, localstack, and http mock
	@./integration-test/run-test.sh

integration-clean: ## Remove Docker images and volumes used by integration tests
	@echo "Cleaning integration test resources..."
	@docker compose down -v --rmi local 2>/dev/null || true

clean: ## Clean vendor directories and installed dependencies
	@echo "Cleaning vendor directories..."
	@rm -rf vendor/bundle vendor/jar-dependencies
	@rm -f Gemfile.lock vendor/bundle/.timestamp vendor/jar-dependencies/.timestamp

dist-clean: clean ## Clean all generated files including vendor directory
	@echo "Performing distribution clean..."
	@rm -rf vendor

setup: install-jars ## Full setup for local development (install + install-jars)
	@echo "Setup complete! Ready for development."
	@echo ""
	@echo "Next steps:"
	@echo "  - Run 'make test' to run tests"
	@echo "  - See 'make help' for more commands"

all: setup ## Alias for setup
