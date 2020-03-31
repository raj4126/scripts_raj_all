
class Pcf(object):
    def __init__(self, pcf=None):
        self._pcf = pcf if pcf else {}

    def get_attribute(self, attr):
        try:
            return self._pcf[attr]
        except KeyError:
            return None

    def has_attribute(self, attr):
        return attr in self._pcf

    @property
    def batch_id(self):
        return self.get_attribute('batch_id')

    @property
    def item_id(self):
        return self.get_attribute('item_id')

    @property
    def trusted_prediction(self):
        return self.get_attribute('trusted_prediction')

    @property
    def title(self):
        return self.get_attribute('title')

    @property
    def long_description(self):
        return self.get_attribute('long_description')

    @property
    def short_description(self):
        return self.get_attribute('short_description')

    @property
    def product_type(self):
        return self.get_attribute('product_type')
