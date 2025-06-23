# Etapa 1: Build do front-end
FROM node:18 AS frontend-build
WORKDIR /app/front-end
COPY front-end/package.json ./
RUN npm install
COPY front-end/ ./
RUN npm run build

# Etapa 2: Build da API
FROM python:3.11-slim AS api-build
WORKDIR /app/rest
COPY rest/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY rest/ ./

# Etapa 3: Container final com Nginx
FROM nginx:alpine
# Copia build do front-end
COPY --from=frontend-build /app/front-end/build /usr/share/nginx/html
# Copia código da API
COPY --from=api-build /app/rest /app/rest
# Copia requirements para referência
COPY rest/requirements.txt /app/rest/requirements.txt

# Instalações para rodar a API
RUN apk add --no-cache python3 py3-pip supervisor
RUN pip3 install --no-cache-dir -r /app/rest/requirements.txt

# Copia configuração do Nginx
COPY nginx.conf /etc/nginx/conf.d/default.conf
# Copia entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 80

ENTRYPOINT ["/entrypoint.sh"]
