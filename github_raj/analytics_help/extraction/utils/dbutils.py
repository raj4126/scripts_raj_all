import logging
from extraction.pipeline.pipe import Pipe
from extraction.logging_utils import log_cassendra_extract_attribute
from cassandra.cluster import Cluster
from cassandra.query import ValueSequence
from cassandra.auth import PlainTextAuthProvider
from extraction.utils.label_level_metrics import LabelLevelMetrics
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement



logger = logging.getLogger()


class DbUtils:
    def __init__(self, connection_info):
        node_ips, namespace, user_id, password = connection_info
        auth_provider = PlainTextAuthProvider(
            username=user_id, password=password)
        self._session = Cluster(node_ips, auth_provider=auth_provider).connect(namespace)

    def insert_model_predictions(self, extracted_attribute):
        query = "insert into ae_batch_predictions1_tbl" \
                "(batch_id,attribute,item_id,title,long_description," \
                "short_description,model_prediction,trusted_prediction) " \
                "VALUES" \
                " (%s,%s,%s,%s,%s,%s,%s,%s)"
        for extracted_attribute in extracted_attribute:
            self._session.execute(query, extracted_attribute)

        return {"Message": "Insertion Successful"}

    def fetch_label_metrics(self, data):
        attribute, batch_id = data
        query = "select item_id,trusted_prediction,model_prediction from attributeextraction.ae_batch_predictions1_tbl " \
                "where  attribute='{}' and batch_id='{}'".format(attribute, batch_id)
        logger.info('Read query : %s', query)
        result = self._session.execute(query)

        return result
