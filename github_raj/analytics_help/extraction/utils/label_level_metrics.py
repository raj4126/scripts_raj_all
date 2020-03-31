

class LabelLevelMetrics:
    def __init__(self,):
        self.source_of_truth_label = []
        self.model_prediction_label = []
        self.item_id = []
        self.label_set = []
        self.obtain_results_for_batch()

    def obtain_results_for_batch(self, batch_db_record):

        for item, trusted_prediction, model_prediction in batch_db_record:
            self.item_id.append(item)
            binary_classifier = int(model_prediction == trusted_prediction)
            if binary_classifier == "0":
                self.source_of_truth_label.append(trusted_prediction)
                self.model_prediction_label.append(model_prediction)
            elif binary_classifier == "1":
                self.source_of_truth_label.append(model_prediction)
                self.model_prediction_label.append(model_prediction)
            else:
                print('the evaluation is not in the usual format!')
        else:
            raise Exception('the evaluation is not in the usual format!')

    def get_all_possible_labels():
        label_set = set()
        for item in truth + pred:
            tokens = re.split(';|__', item)
            for token in tokens:
                label_set.add(token)
        return sorted(list(label_set - {None, ""}))

    def is_label_in_multi_label(label, multilabel):
        return int((label in multilabel.split("__")) or (label in multilabel.split(";")))

    def get_metrics(truth, pred, label):
        y_truth = [is_label_in_multi_label(label, x) for x in truth]
        y_pred = [is_label_in_multi_label(label, x) for x in pred]
        if all([x == 0 for x in y_truth]) or all([x == 0 for x in y_pred]):
            return [0.0, 0.0, 0.0, 0.0]
        precision = precision_score(y_truth, y_pred)
        recall = recall_score(y_truth, y_pred)
        f1 = f1_score(y_truth, y_pred)
        support = sum(y_truth)
        return [round(x, 3) for x in [precision, recall, f1, support]]


    def fetch_metrics(self):
        self.label_set = self.get_all_possible_labels()
