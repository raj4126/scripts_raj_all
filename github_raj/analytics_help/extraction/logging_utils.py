from functools import wraps
import time
import logging

logger = logging.getLogger()


def log_time(func):
    @wraps(func)
    def with_logging(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        lapsed = time.time() - start

        logger.info(
            'Time lapse by %s() is [float_timeLapse=>%s]',
            func.__name__,
            lapsed
        )
        return result

    return with_logging


def log_cassendra_extract_attribute(func):
    @wraps(func)
    def with_logging(self, attributes, pcf):
        start = time.time()
        results = func(self, attributes, pcf)
        lapsed = time.time() - start

        logger.info(
            'Time lapse by %s for %s extracting [int_attributes=>%s] is [float_timeLapse=>%s]',
            func.__name__,
            attributes,
            len(results.items()),
            lapsed
        )
        return results

    return with_logging


def log_extract_attribute(func):
    @wraps(func)
    def with_logging(self, attributes, pcf):
        start = time.time()
        results = func(self, attributes, pcf)
        lapsed = time.time() - start

        times = results['lapse_times'] if 'lapse_times' in results else {}
        results = {(pcf.batch_id,
                    prediction['attribute'],
                    pcf.item_id,
                    pcf.title,
                    pcf.long_description,
                    pcf.short_description,
                    ','.join(prediction['prediction']),
                    ','.join(pcf.trusted_prediction)) for prediction in results['predictions']} \
            if 'predictions' in results else {}

        for attr, lapse_time in times.items():
            logger.info(
                'Time lapse for extracting [attribute=>%s] from [processing_host=>%s] is [float_timeLapse=>%s]',
                attr,
                lapse_time['host'],
                lapse_time['lapse_time']
            )
        logger.info(
            'Time lapse by %s for %s extracting [int_attributes=>%s] is [float_timeLapse=>%s]',
            func.__name__,
            attributes,
            len(results),
            lapsed
        )
        return list(results)

    return with_logging
