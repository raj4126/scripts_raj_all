
import abc
import logging
from extraction.pipeline.coroutine import coroutine

logger = logging.getLogger()


class Pipe:
    __metaclass__ = abc.ABCMeta

    def __init__(self, context, targets=None):
        self._context = context
        self._targets = targets if targets else []
        self._co = self.start()

    @abc.abstractmethod
    def handle(self, data):
        pass

    @property
    def error(self):
        return self._context['error']

    def send(self, data):
        self._co.send(data)

    @coroutine
    def start(self):
        while True:
            try:
                data = (yield)
                results = self.handle(data)
                if results:
                    self.send_to_all_targets(results)
            except Exception as e:
                self._context['error'] = e
                logger.exception('Error processing message: [error=>%s]', e.message)

    def send_to_all_targets(self, results):
        for target in self._targets:
            target.send(results)
