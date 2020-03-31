import logging
from extraction.pipeline.pipe import Pipe
from extraction.logging_utils import log_cassendra_extract_attribute
from cassandra.cluster import Cluster
from cassandra.query import ValueSequence
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from extraction.utils.dbutils import DbUtils


logger = logging.getLogger()


class WritePipe(Pipe):
    def __init__(self, context, db_session, targets=None):
        Pipe.__init__(self, context, targets)
        self._db_session = db_session

    def handle(self, extracted_attributes):
        results = self.insert_model_predictions(extracted_attributes)
        logger.info('Extracted data record : %s', extracted_attributes)
        return results

    def insert_model_predictions(self, extracted_attributes):
        return self._db_session.insert_model_predictions(extracted_attributes)