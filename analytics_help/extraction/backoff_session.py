
import logging
import backoff
import requests
from extraction.exceptions import PostException

logger = logging.getLogger()


class BackoffSession(requests.Session):

    def __init__(self):
        requests.Session.__init__(self)

    @backoff.on_exception(backoff.expo, PostException, max_tries=3, jitter=backoff.full_jitter)
    @backoff.on_predicate(backoff.expo, predicate=lambda x: x.status_code in [404, 408, 500], max_tries=3, jitter=backoff.full_jitter)
    def post(self, url, data=None, json=None, **kwargs):
        try:
            return requests.Session.post(self, url, data=data, json=json, **kwargs)
        except requests.ConnectTimeout as e:
            # retried
            logger.exception('post request failed: %s', e.message)
            raise PostException(e)
        except requests.ReadTimeout as e:
            # retried
            logger.exception('post request failed: %s', e.message)
            raise PostException(e)
        except requests.RequestException as e:
            # retried
            logger.exception('post request failed: %s', e.message)
            raise PostException(e)
        except Exception as e:
            if 'Connection reset by peer' in e.message \
                    or 'Failed to establish a new connection' in e.message:
                # retried
                raise PostException(e)
            else:
                # not retried
                logger.exception('post request failed: %s', e.message)
                raise
