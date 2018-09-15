import json
import logging
from extraction.pipeline.pipe import Pipe
from extraction.logging_utils import log_extract_attribute
from extraction.exceptions import PostException

logger = logging.getLogger()



class TenantExtractionPipe(Pipe):
    def __init__(self, context, connection_info, targets=None):
        Pipe.__init__(self, context, targets)
        self._host, session = connection_info
        self._session, self._connection_timeout, self._read_timeout = session

    def handle(self, data):
        pcf, model_predictions = data
        extracted_attributes = self.extract_attribute(model_predictions, pcf)
        logger.info('Extracted item %s attribute: %s', pcf.item_id, extracted_attributes)
        return pcf, extracted_attributes

    @log_extract_attribute
    def extract_attribute(self, attribute, pcf):
        tenant_extracted = {}

        if attribute:
            logger.info('Getting predictions for attributes %s', attribute)

            payload = "(product_attributes.item_id.values.value=={}) ".format(pcf.item_id)

            logger.info("Extracted item from Tenant  %s", pcf.item_id)
            #payload["docs"][0]["product_attributes"][attribute]["values"][0]["value"])

            try:
                response = self._session.post(
                    self._host,
                    data=payload,
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
                    tenant_extracted = response.json()
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
                            'Get prediction request failed, status code:%s, response: %s, payload:%s, [error=>%s]',
                            response.status_code,
                            response.content,
                            payload,
                            error
                        )

                        if response.status_code == 400:
                            raise Exception(error)  # data error
                        else:
                            raise PostException(Exception(error))

        return tenant_extracted
