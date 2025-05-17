IMAGE_NAME=jupyter-custom
CONTAINER_NAME=jupyterlab
PORT=8888
LOCAL_NOTEBOOKS=./notebooks
CONTAINER_NOTEBOOKS=/home/jovyan/work

.PHONY: build run stop

build:
	docker build -t $(IMAGE_NAME) .

run:
	# Cria o diretório local se não existir
	mkdir -p $(LOCAL_NOTEBOOKS)
	# Executa o container com o volume montado
	docker run -d --rm \
		--name $(CONTAINER_NAME) \
		-p $(PORT):8888 \
		-v $(LOCAL_NOTEBOOKS):$(CONTAINER_NOTEBOOKS) \
		$(IMAGE_NAME)

stop:
	docker stop $(CONTAINER_NAME)
