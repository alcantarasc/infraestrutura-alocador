import os

# SERVICES

## Persistency
DATABASE_PROVIDER = os.environ.get('DATABASE_PROVIDER', 'postgresql')
DATABASE_ADAPTER = os.environ.get('DATABASE_ADAPTER', 'psycopg2')
DATABASE_NAME = os.environ.get('DATABASE_NAME', 'postgres')
DATABASE_USER = os.environ.get('DATABASE_USER', 'postgres')
DATABASE_PASSWORD = os.environ.get('DATABASE_PASSWORD', 'postgres')
DATABASE_HOST = os.environ.get('DATABASE_HOST', 'localhost')
DATABASE_PORT = os.environ.get('DATABASE_PORT', '5432')

## Firebase
FIREBASE_CREDENTIAL_FILE = os.environ.get('FIREBASE_CREDENTIAL_FILE', "/app/rest/firebase-credential.json")
