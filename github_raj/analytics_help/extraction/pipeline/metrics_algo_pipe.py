from sklearn.metrics import precision_score, recall_score, f1_score
import logging
from extraction.pipeline.pipe import Pipe
import re


logger = logging.getLogger()


class MetricsAlgoPipe(Pipe):
    def __init__(self, context, targets=None):
        Pipe.__init__(self, context, targets)

    def handle(self, data):
        extracted_attribute = self.extract_attribute(data)
        logger.info('')
        return extracted_attribute

    def extract_attribute(self, data):
        response_json = {}
        item_ids, source_of_truth_labels, model_prediction_labels = data
        label_set = self.get_all_possible_labels(source_of_truth_labels + model_prediction_labels)
        for label in label_set:
            metrics = self.get_metrics(source_of_truth_labels, model_prediction_labels, label)
            response_json[label] = "\t".join(str(x) for x in metrics)

        return response_json

    @staticmethod
    def get_all_possible_labels(labels):
        label_set = set()
        for label in labels:
            tokens = re.split(';|__', label)
            for token in tokens:
                label_set.add(token)
        return sorted(list(label_set - {None, ""}))

    @staticmethod
    def is_label_in_multi_label(label, multilabel):
        return int((label in multilabel.split("__")) or (label in multilabel.split(";")))

    def get_metrics(self, truth, pred, label):
        y_truth = [self.is_label_in_multi_label(label, x) for x in truth]
        y_pred = [self.is_label_in_multi_label(label, x) for x in pred]
        if all([x == 0 for x in y_truth]) or all([x == 0 for x in y_pred]):
            return [0.0, 0.0, 0.0, 0.0]
        precision = precision_score(y_truth, y_pred)
        recall = recall_score(y_truth, y_pred)
        f1 = f1_score(y_truth, y_pred)
        support = sum(y_truth)
        return [round(x, 3) for x in [precision, recall, f1, support]]
