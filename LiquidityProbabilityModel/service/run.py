import os

from distutils.util import strtobool

import flask
from flask import jsonify

import models as ml
from errors import InvalidUsage


app = flask.Flask(__name__)


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify({
        'error': error.to_dict()
    })
    response.status_code = error.status_code
    return response


@app.route('/')
def index():
    return jsonify('Welcome to Quality Score API!')


@app.route('/predict', methods=['POST'])
def predict():
    request = flask.request.get_json()
    market = request.get('market', os.environ.get('MAIN_MARKET'))
    section = request.get('section', os.environ.get('MAIN_SECTION'))

    if not ml.know(market, section):
        raise InvalidUsage('Not supported "market" or "section"')

    try:
        explain = bool((request.get('explain', '0')))

    except ValueError:
        raise InvalidUsage('Parameter "explain" must be boolean')

    m = ml.get(market, section)
    print(m.name())
    fb = m.features_builder

    try:
        features = fb.create(request['properties'][0])

    except Exception as e:
        raise InvalidUsage(str(e))

    response = {
        'model': m.name(),
        'probability': m.predict(features)
    }

    if explain:
        response['explanation'] = m.explain(features)

    return jsonify(response)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
