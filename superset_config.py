import os

SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'unsafe-default-secret-key')
