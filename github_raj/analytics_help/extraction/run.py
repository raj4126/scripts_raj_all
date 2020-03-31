
from flask import request, Response
from extraction.app import App
import json
import socket
import logging
from extraction.pcf import Pcf
import traceback


application = App(__name__)
application.load_configs()
application.config_loggers()
application.load_db_connection()


try:
    host_ip = socket.gethostbyname(socket.gethostname())
except:
    host_ip = 'unknown'

logger = logging.getLogger(__name__)

session = None
prepared = None


@application.route('/health', methods=['GET', 'POST'])
def health_check():
    output = {'host': host_ip}
    try:
        output['status'] = 'OK'
        output['message'] = 'Service is up'
        return Response(json.dumps(output), status=200, mimetype='application/json')
    except Exception as e:
        output['status'] = 'KO'
        output['exception'] = unicode(e)
        return Response(json.dumps(output), status=500, mimetype='application/json')


@application.route('/predictions', methods=['POST'])
def predictions():
    try:
        logger.info(request.data)
        pcf = Pcf(json.loads(request.data))
    except Exception as e:
        traceback.print_exc()
        logger.exception('error encountered parsing pcf: %s', request.data)
        output = {'status': 'KO', 'error': unicode(e.message), 'host': host_ip}
        return Response(json.dumps(output), status=400, mimetype='application/json')

    try:
        attributes = request.headers['X-Attr'] if 'X-Attr' in request.headers else ''
        extracted_attributes = application.extract_attributes(pcf, attributes)
        logger.debug('payload: %s', request.data)
        logger.info('extracted attributes %s for item %s', extracted_attributes, pcf.item_id)
        return Response(json.dumps(extracted_attributes), status=200, mimetype='application/json')
    except Exception as e:
        traceback.print_exc()
        logger.exception('error encountered during extraction for item: %s', pcf.item_id)
        output = {'status': 'KO', 'error': unicode(e.message), 'host': host_ip}
        return Response(json.dumps(output), status=500, mimetype='application/json')


@application.route('/pr/<batch_id>/<attribute>', methods=['GET'])
def label_metrics(attribute, batch_id):
    try:
        metrics_results = application.get_label_metrics(attribute, batch_id)
        logger.info('Metrics results %s ', metrics_results)
        if not metrics_results:
            metrics_results = {"error:":"Invalid batch/attribute combination"}
        return Response(json.dumps(metrics_results), status=200, mimetype='application/json')
    except Exception as e:
        traceback.print_exc()
        logger.exception('error encountered while fetching metrics results: attribute=%s ,batch_id=%s', attribute, batch_id)
        output = {'status': 'KO', 'error': unicode(e.message), 'host': host_ip}
        return Response(json.dumps(output), status=500, mimetype='application/json')


if __name__ == "__main__":
    application.run(host='0.0.0.0', port=5003)
