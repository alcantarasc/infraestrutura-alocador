.PHONY: install start stop restart logs clean deploy

install:
	@echo "Installing Docker and Docker Compose..."
	sudo apt-get update
	sudo apt-get install -y docker.io docker-compose ufw
	sudo systemctl enable docker
	sudo systemctl start docker
	sudo usermod -aG docker ${USER}
	@echo "Installation complete! Please log out and log back in for group changes to take effect."

deploy:
	@echo "Configuring firewall for deployment..."
	sudo ufw allow 22/tcp
	sudo ufw allow 80/tcp
	sudo ufw allow 5432/tcp
	sudo ufw allow 9200/tcp
	sudo ufw allow 8000/tcp
	@echo "Enable UFW firewall? This might disconnect your SSH session."
	@read -p "Press Enter to continue or Ctrl+C to cancel..."
	sudo ufw enable
	@echo "Starting services in deployment mode..."
	docker-compose pull
	docker-compose up -d
	@echo "Deployment complete! Services are now accessible:"
	@echo "- API (via Nginx): http://<server-ip>:80"
	@echo "- API (direct): http://<server-ip>:8000"
	@echo "- PostgreSQL: <server-ip>:5432"
	@echo "- OpenSearch: http://<server-ip>:9200"
	@echo "Remember to:"
	@echo "1. Replace <server-ip> with your actual server IP"
	@echo "2. Secure your services with proper authentication"
	@echo "3. Consider setting up SSL/TLS for production use"

start:
	docker-compose up -d

stop:
	docker-compose down

restart: stop start

logs:
	docker-compose logs -f

clean:
	docker-compose down -v
	docker system prune -f 