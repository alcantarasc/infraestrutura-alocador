sudo docker build -t app-alocador .
sudo docker run -d -p 80:80 --name app-alocador app-alocador



# Para o container atual
sudo docker stop app-alocador
sudo docker rm app-alocador

# Rebuild com as modificações
sudo docker build -t app-alocador .

# Roda novamente
sudo docker run -d -p 80:80 --name app-alocador app-alocador

# Verifica os logs para confirmar que está funcionando
sudo docker logs app-alocador