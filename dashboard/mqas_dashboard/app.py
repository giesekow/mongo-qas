from flask import Flask, redirect, url_for

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)

    # a simple page that says hello
    @app.route('/health')
    def health():
        return "Application is Running"


    @app.route('/')
    def index():
        return redirect(url_for('jobs.items'))


    from . import workers
    app.register_blueprint(workers.bp)

    from . import jobs
    app.register_blueprint(jobs.bp)

    return app