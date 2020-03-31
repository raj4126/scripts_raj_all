
import json
import logging
import logging.config
from extraction.backoff_session import BackoffSession
from logging.handlers import TimedRotatingFileHandler
import os
from extraction.pipeline.model_extraction_pipe import ModelExtractionPipe
from extraction.pipeline.db_write_pipe import WritePipe
from extraction.pipeline.db_read_pipe import ReadPipe
from extraction.pipeline.metrics_algo_pipe import MetricsAlgoPipe
from extraction.utils.dbutils import DbUtils

from extraction.pipeline.null_sink import NullSink

from flask import Flask
import requests

logger = logging.getLogger()


def set_run_on_env_or_raise():
    if 'runOnEnv' not in os.environ:
        json_file = os.path.join('/', 'app', 'ae-measure', 'runOnEnv.json')
        if os.path.exists(json_file) and os.path.isfile(json_file):
            with open(json_file, 'r') as fs:
                run_on_env_json = json.load(fs)
                if 'runOnEnv' in run_on_env_json:
                    os.environ['runOnEnv'] = run_on_env_json['runOnEnv'].lower()
                if 'runOnCloud' in run_on_env_json:
                    os.environ['runOnCloud'] = run_on_env_json['runOnCloud'].lower()
        if 'runOnEnv' not in os.environ:
            raise OSError('runOnEnv is not defined in environmnet nor in {}'.format(json_file))
        if 'runOnCloud' not in os.environ:
            raise OSError('runOnCloud is not defined in environmnet nor in {}'.format(json_file))


class App(Flask):
    def __init__(self, name):
        Flask.__init__(self, name)
        self._db_session = None
        self._requests_session = BackoffSession()
        self._requests_session.mount('http://', requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10))

    def config_loggers(self):
        logging.config.fileConfig('{}/logging.conf'.format(os.path.dirname(__file__)))
        for handler in logging.root.handlers:
            if isinstance(handler, TimedRotatingFileHandler):
                self.logger.addHandler(handler)
                self.logger.info('Configured application logger to rotatingFileHandler.')
                logging.getLogger('backoff').addHandler(handler)
                break
        logging.getLogger('backoff').setLevel(logging.INFO)
        self.logger.info('finished configuring loggers')

    def get_database_connection(self):
        try:
            if not self._db_session:
                connection_info = (self.config['CASSANDRA_NODE_IPS'],
                                   self.config['CASSANDRA_KEYSPACE'],
                                   self.config['CASSANDRA_USERNAME'],
                                   self.config['CASSANDRA_PASSWORD'])
                self._db_session = DbUtils(connection_info)
        except Exception as e:
            logger.exception('Database initialization failed - %s', e.message)
            raise

    def load_configs(self):
        set_run_on_env_or_raise()
        self.logger.info('starting in environment: %s, cloud: %s', os.environ['runOnEnv'], os.environ['runOnCloud'])
        self.config.from_pyfile('{}/config/default_config.py'.format(os.path.dirname(__file__)))
        self.config.from_pyfile(
            '{}/config/config_{}.py'.format(os.path.dirname(__file__), os.environ['runOnEnv']))
        self.logger.info('loaded configurations from: %s, cloud: %s', os.environ['runOnEnv'],
                         os.environ['runOnCloud'])

    def load_db_connection(self):
        self.get_database_connection()

    def extract_attributes(self, pcf, attributes):
        context = {}
        write_data_sink = NullSink(context)
        db_write_pipe = WritePipe(context, self._db_session, [write_data_sink])
        insert_metrics_pipeline = ModelExtractionPipe(
            context,
            (self.config['MODEL_HOST'],
             (self._requests_session, self.config['CONNECTION_TIMEOUT'], self.config['READ_TIMEOUT'])),
            [db_write_pipe]
        )

        insert_metrics_pipeline.send((pcf, attributes))
        result = write_data_sink.data if write_data_sink.data else write_data_sink.error
        return result

    def get_label_metrics(self, attributes, batch_id):
        context = {}
        read_data_sink = NullSink(context)
        metrics_algo_pipe = MetricsAlgoPipe(context, [read_data_sink])
        fetch_metrics_pipeline = ReadPipe(context, self._db_session, [metrics_algo_pipe])
        fetch_metrics_pipeline.send((attributes, batch_id))
        result = read_data_sink.data if read_data_sink.data else None
        return result
