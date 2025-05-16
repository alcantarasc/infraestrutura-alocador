.PHONY: build run setup-users

# Variables
CONTAINER_NAME := jupyter-lab
USERS := lucas martim
PORT := 8888

build:
	@echo "Building Docker image..."
	docker build -t $(CONTAINER_NAME) .

run:
	@echo "Starting JupyterLab container..."
	docker run -d \
		--name $(CONTAINER_NAME) \
		-p $(PORT):8888 \
		-v $(PWD)/notebooks:/home/jovyan/work \
		$(CONTAINER_NAME)

setup-users:
	@echo "Setting up users..."
	@for user in $(USERS); do \
		PASSWORD=$$(openssl rand -base64 12); \
		echo "Creating user: $$user with password: $$PASSWORD"; \
		docker exec $(CONTAINER_NAME) bash -c "useradd -m $$user && echo $$user:$$PASSWORD | chpasswd"; \
	done

stop:
	@echo "Stopping container..."
	docker stop $(CONTAINER_NAME)
	docker rm $(CONTAINER_NAME)

all: build run setup-users 