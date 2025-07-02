sudo docker build -t app-alocador .
sudo docker run -d -p 80:80 --name app-alocador app-alocador