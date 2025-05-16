.PHONY: build run setup-users stop clean setup-user build-and-run

# Variables
IMAGE_NAME = jupyterlab
CONTAINER_NAME = jupyterlab-container
PORT = 8888
USERS := lucas martim

build:
	@echo "Building Docker image..."
	docker build -t $(IMAGE_NAME) .

run:
	@echo "Starting JupyterLab container..."
	docker run -d \
		--name $(CONTAINER_NAME) \
		-p $(PORT):8888 \
		-v $(PWD)/notebooks:/home/jovyan/work \
		$(IMAGE_NAME)
	@echo "Waiting for container to be ready..."
	@sleep 5

setup-users:
	@echo "Setting up JupyterLab users..."
	@for user in $(USERS); do \
		PASSWORD=$$(openssl rand -base64 12); \
		echo "Creating JupyterLab user: $$user with password: $$PASSWORD"; \
		docker exec $(CONTAINER_NAME) jupyter server auth add $$user $$PASSWORD; \
	done

stop:
	@echo "Stopping container..."
	docker stop $(CONTAINER_NAME) || true
	docker rm $(CONTAINER_NAME) || true

clean: stop
	docker rmi $(IMAGE_NAME) || true

setup-user:
	@if [ -z "$(CONTAINER_NAME)" ]; then \
		echo "Error: CONTAINER_NAME is required"; \
		exit 1; \
	fi
	./setup-user.sh $(CONTAINER_NAME)

build-and-run: build
	docker run --name $(CONTAINER_NAME) -p $(PORT):8888 $(IMAGE_NAME)
	@echo "Container stopped. You can remove it with 'make stop'"

all: build run setup-users 