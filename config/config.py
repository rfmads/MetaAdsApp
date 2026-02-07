# config/config.py
import os

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "s1s2s3s4s5")
DB_NAME = os.getenv("DB_NAME", "MetaAdsdb")

META_USER_TOKEN = os.getenv("META_USER_TOKEN")
META_GRAPH_VERSION = os.getenv("META_GRAPH_VERSION", "v22.0")
