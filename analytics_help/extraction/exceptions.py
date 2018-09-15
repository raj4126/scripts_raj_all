
class MissingItemIdException(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)


class PostException(Exception):
    def __init__(self, original_exception):
        self._original_exception = original_exception

    @property
    def message(self):
        msg = self._original_exception.message if self._original_exception else None

        if not msg:
            return None

        if 'Connection reset by peer' in msg:
            msg = 'Connection reset by peer'
        elif 'Failed to establish a new connection' in msg:
            msg = 'Failed to establish a new connection'

        return msg


