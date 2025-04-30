#!/bin/bash

AIRFLOW_HOME=$HOME/airflow
VENV_PATH=$AIRFLOW_HOME/venv

# Function to activate virtual environment
activate_venv() {
    source $VENV_PATH/bin/activate
}

# Function to start services
start_services() {
    echo "Starting Airflow services..."
    sudo systemctl start airflow-webserver.service
    sudo systemctl start airflow-scheduler.service
    echo "Airflow services started."
}

# Function to stop services
stop_services() {
    echo "Stopping Airflow services..."
    sudo systemctl stop airflow-webserver.service
    sudo systemctl stop airflow-scheduler.service
    echo "Airflow services stopped."
}

case "$1" in
    start)
        activate_venv
        start_services
        ;;
    stop)
        activate_venv
        stop_services
        ;;
    restart)
        activate_venv
        stop_services
        start_services
        ;;
    *)
        echo "Usage: $0 {start|stop|restart}"
        exit 1
        ;;
esac

exit 0
