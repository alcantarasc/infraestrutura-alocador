https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.1/docker-compose.yaml'


mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker compose up airflow-init

docker compose up

docker compose down --volumes --remove-orphans