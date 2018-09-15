import logging
from extraction.pipeline.pipe import Pipe


logger = logging.getLogger()


class ReadPipe(Pipe):
    def __init__(self, context, db_session, targets=None):
        Pipe.__init__(self, context, targets)
        self._db_session = db_session

    def handle(self, data):
        algo_input_data = self.fetch_model_predictions(data)
        logger.info('Algo input data retrieved')
        return algo_input_data

    def fetch_model_predictions(self, data):
        source_of_truth_labels = []
        model_prediction_labels = []
        item_ids = []

        results = self._db_session.fetch_label_metrics(data)

        for result in results:
            item_id = result.item_id if result.item_id else ""
            trusted_prediction = result.trusted_prediction if result.trusted_prediction else ""
            model_prediction = result.model_prediction if result.model_prediction else ""

            item_ids.append(item_id)
            binary_classifier = int(model_prediction == trusted_prediction)
            if binary_classifier == 0:
                source_of_truth_labels.append(trusted_prediction)
                model_prediction_labels.append(model_prediction)
            elif binary_classifier == 1:
                source_of_truth_labels.append(model_prediction)
                model_prediction_labels.append(model_prediction)
            else:
                logger.error('binary classifying prediction !')

        return item_ids, source_of_truth_labels, model_prediction_labels
