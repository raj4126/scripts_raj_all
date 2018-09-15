
import logging
from extraction.pipeline.pipe import Pipe

logger = logging.getLogger()


class NullSink(Pipe):
    def __init__(self, context, targets=None):
        Pipe.__init__(self, context, targets)
        self._data = None

    @property
    def data(self):
        return self._data

    def handle(self, data):
        self._data = data
