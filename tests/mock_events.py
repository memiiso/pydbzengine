from pydbzengine import ChangeEvent


class MockChangeEvent(ChangeEvent):
    """
    A concrete implementation of ChangeEvent for testing.
    """

    def __init__(self, key: str, value: str, destination: str, partition: int=1):
        self._key = key
        self._value = value
        self._destination = destination
        self._partition = partition

    def key(self) -> str:
        """Returns the record key."""
        return self._key

    def value(self) -> str:
        """Returns the record value (payload)."""
        return self._value

    def destination(self) -> str:
        """Returns the destination topic/table."""
        return self._destination

    def partition(self) -> int:
        """Returns the partition the record belongs to."""
        return self._partition
