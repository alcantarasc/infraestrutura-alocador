docker build -t frontend-dev -f front-end/Dockerfile.dev front-end
docker run -it --rm -p 3000:3000 -v ${PWD}/front-end:/app frontend-dev