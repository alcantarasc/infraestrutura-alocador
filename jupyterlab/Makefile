IMAGE_NAME=jupyterlab-custom
CONTAINER_NAME=jupyterlab
PORT=8888

.PHONY: build run stop

build:
	docker build -t $(IMAGE_NAME) .

run:
	docker run -d --rm --name $(CONTAINER_NAME) -p $(PORT):8888 $(IMAGE_NAME)

stop:
	docker stop $(CONTAINER_NAME)
