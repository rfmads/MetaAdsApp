from flask import Flask
from api.resources.health import health_bp
from api.resources.config import config_bp
# from api.resources.pipeline import pipeline_bp
from api.resources.jobs import jobs_bp
from api.resources.ads import ads_bp
from api.resources.rfmdata import rfmdata

def create_app():
    app = Flask(__name__)
    # =========================
    # REGISTER BLUEPRINTS (Standard Flask style)
    # =========================
    app.register_blueprint(ads_bp)
    app.register_blueprint(health_bp)
    app.register_blueprint(config_bp)
    # app.register_blueprint(pipeline_bp)
    app.register_blueprint(jobs_bp)
    app.register_blueprint(rfmdata)
    return app

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True, use_reloader=False )