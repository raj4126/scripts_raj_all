
import json
import logging
from extraction.pipeline.pipe import Pipe
from extraction.logging_utils import log_extract_attribute
from extraction.exceptions import PostException


logger = logging.getLogger()


class ModelExtractionPipe(Pipe):
    def __init__(self, context, connection_info, targets=None):
        Pipe.__init__(self, context, targets)
        self._host, session = connection_info
        self._session, self._connection_timeout, self._read_timeout = session

    def handle(self, data):
        pcf, attribute = data
        extracted_attribute = self.extract_attribute(attribute, pcf)
        logger.info('Extracted item %s attribute: %s', pcf.item_id, extracted_attribute)
        return extracted_attribute

    @staticmethod
    def included_attribute():
        return [
            ('title', 'title'),
            ('short_description', 'short_description'),
            ('medium_description', 'medium_description'),
            ('long_description', 'long_description'),
            ('product_type', 'product_type'),
        ]

    @log_extract_attribute
    def extract_attribute(self, attribute, pcf):
        model_predictions = {}

        if attribute:
            logger.info('Getting predictions for attribute %s', attribute)
            url = 'http://{}/predictions'.format(self._host)
            headers = {
                'Content-Type': 'text/plain',
                'X-Attr': attribute
            }

            payload = {
                attribute[1]: pcf.get_attribute(attribute[0])
                for attribute in self.included_attribute() if pcf.has_attribute(attribute[0])
            }


            logger.info(payload)

            try:
                response = self._session.post(
                    url,
                    headers=headers,
                    data=json.dumps(payload),
                    timeout=(self._connection_timeout, self._read_timeout)
                )
            except Exception as e:
                logger.exception(
                    'Get prediction request failed, attribute: %s, [error=>Prediction failure - %s]',
                    attribute,
                    e.message
                )
                raise
            else:
                if response.ok:
                    model_predictions = response.json()
                else:
                    try:
                        error = json.loads(response.content)['error']
                        if 'Connection reset by peer' in error:
                            error = 'Connection reset by peer'
                        elif 'Failed to establish a new connection' in error:
                            error = 'Failed to establish a new connection'
                        else:
                            error = '{}'.format(error)
                    except:
                        error = 'cannot parse error'
                    finally:
                        logger.error(
                            'Get prediction request failed, status code:%s, response: %s, headers %s, payload:%s, [error=>%s]',
                            response.status_code,
                            response.content,
                            headers,
                            payload,
                            error
                        )

                        if response.status_code == 400:
                            raise Exception(error)  # data error
                        else:
                            raise PostException(Exception(error))

        return model_predictions
