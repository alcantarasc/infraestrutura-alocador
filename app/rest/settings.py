import os

# SERVICES

## Persistency
DATABASE_PROVIDER = os.environ.get('DATABASE_PROVIDER', 'postgresql')
DATABASE_ADAPTER = os.environ.get('DATABASE_ADAPTER', 'psycopg2')
DATABASE_NAME = os.environ.get('DATABASE_NAME', 'screening_cvm')
DATABASE_USER = os.environ.get('DATABASE_USER', 'DATABASE_USER')
DATABASE_PASSWORD = os.environ.get('DATABASE_PASSWORD', 'DATABASE_PASSWORD')
DATABASE_HOST = os.environ.get('DATABASE_HOST', 'DATABASE_HOST')
DATABASE_PORT = os.environ.get('DATABASE_PORT', 'DATABASE_PORT')

## Firebase
FIREBASE_CREDENTIAL_FILE = os.environ.get('FIREBASE_CREDENTIAL_FILE', "/app/rest/firebase-credential.json")
