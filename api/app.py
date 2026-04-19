from flask import Flask
from flask_smorest import Api
from flask_cors import CORS

# your blueprints
from api.resources.health import blp as health_blp
from api.resources.config import blp as config_blp
from api.resources.pipeline import blp as pipeline_blp
from api.resources.jobs import blp as jobs_blp
from api.resources.ads import blp as ads_blp


def create_app():
    app = Flask(__name__)
    CORS(app)

    # =========================
    # REQUIRED FLASK-SMOREST CONFIG
    # =========================
    app.config.update({
        # OpenAPI basics
        "API_TITLE": "Meta Ads Pipeline API",
        "API_VERSION": "1.0.0",
        "OPENAPI_VERSION": "3.0.3",

        # MUST be "/" (not empty, not missing)
        "OPENAPI_URL_PREFIX": "/",

        # Swagger UI
        "OPENAPI_SWAGGER_UI_PATH": "/apidocs/",
        "OPENAPI_SWAGGER_UI_URL": "https://cdn.jsdelivr.net/npm/swagger-ui-dist/",

        # ReDoc (optional but safe)
        "OPENAPI_REDOC_PATH": "/redoc/",
        "OPENAPI_REDOC_URL": "https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js",

        # IMPORTANT FIX (this is usually missing)
        "PROPAGATE_EXCEPTIONS": True,
    })

    # =========================
    # INIT API
    # =========================
    api = Api(app)

    # =========================
    # REGISTER BLUEPRINTS
    # =========================
    api.register_blueprint(ads_blp)
    api.register_blueprint(health_blp)
    api.register_blueprint(config_blp)
    api.register_blueprint(pipeline_blp)
    api.register_blueprint(jobs_blp)

    return app


app = create_app()


if __name__ == "__main__":
    # app.run(host="127.0.0.1", port=5001, debug=True)
    app.run(host="0.0.0.0", port=5001)