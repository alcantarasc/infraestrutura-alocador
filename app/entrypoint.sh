#!/bin/sh
# Start FastAPI (Uvicorn) in the background
python3 /app/rest/main.py &
# Start Nginx in the foreground
nginx -g 'daemon off;' 