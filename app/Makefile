# Makefile para o aplicativo alocador

# Variáveis
IMAGE_NAME = app-alocador
CONTAINER_NAME = app-alocador
PORT = 80

# Comandos principais
.PHONY: build run stop clean logs help

# Build da imagem
build:
	@echo "Construindo imagem Docker..."
	sudo docker build -t $(IMAGE_NAME) .

# Executar o container
run:
	@echo "Executando container..."
	sudo docker run -d -p $(PORT):$(PORT) --name $(CONTAINER_NAME) $(IMAGE_NAME)

# Parar e remover container atual
stop:
	@echo "Parando e removendo container atual..."
	sudo docker stop $(CONTAINER_NAME) || true
	sudo docker rm $(CONTAINER_NAME) || true

# Rebuild completo (stop + build + run)
rebuild: stop build run
	@echo "Rebuild completo concluído!"

# Verificar logs
logs:
	@echo "Verificando logs do container..."
	sudo docker logs $(CONTAINER_NAME)

# Limpar tudo (container e imagem)
clean:
	@echo "Limpando container e imagem..."
	sudo docker stop $(CONTAINER_NAME) || true
	sudo docker rm $(CONTAINER_NAME) || true
	sudo docker rmi $(IMAGE_NAME) || true

# Executar em modo interativo (para debug)
debug:
	@echo "Executando em modo debug..."
	sudo docker run -it --rm -p $(PORT):$(PORT) $(IMAGE_NAME) /bin/sh

# Verificar status do container
status:
	@echo "Status do container:"
	sudo docker ps -a | grep $(CONTAINER_NAME) || echo "Container não encontrado"

# Ajuda
help:
	@echo "Comandos disponíveis:"
	@echo "  make build    - Construir imagem Docker"
	@echo "  make run      - Executar container"
	@echo "  make stop     - Parar e remover container atual"
	@echo "  make rebuild  - Rebuild completo (stop + build + run)"
	@echo "  make logs     - Verificar logs do container"
	@echo "  make clean    - Limpar tudo (container e imagem)"
	@echo "  make debug    - Executar em modo interativo"
	@echo "  make status   - Verificar status do container"
	@echo "  make help     - Mostrar esta ajuda"

# Comando padrão
.DEFAULT_GOAL := help 