# config/config.example.py
import os

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "a1a2a3a4a5")
DB_NAME = os.getenv("DB_NAME", "MetaAdsdb")

META_USER_TOKEN = os.getenv("META_USER_TOKEN", "EAAH8IfiHPkUBQZCuKKWzOlvwjQm23dIVujIjruO5QdT6BydgoQyZA9FZBSgeJuab0I7MfcKsY6vyrOWb0HEhHRt2x98yfFfWShTncZCzivWDNZAQRZAYCVgarfA0aOUcn7N8YNe1wG0BK4wb02ZANLnF1zZBaBC5k9nrZB581wzV86bkbu6dYDH4tLsnRIhZCo")
META_GRAPH_VERSION = os.getenv("META_GRAPH_VERSION", "v22.0")
