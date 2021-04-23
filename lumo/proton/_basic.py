from typing import Optional, Iterable, BinaryIO
from typing import TypeVar
from abc import abstractmethod

from lumo.codecs import *

__all__ = 'MultipartEncoder', 'MultipartDecoder', \
          'RawEncoder', 'RawDecoder', \
          'VarintEncoder', 'VarintDecoder',

#

_T = TypeVar('_T')


#


class MultipartEncoder(Encoder[_T]):
    def __init__(self, encoders: Optional[Iterable[Encoder]] = None):
        if encoders is not None:
            self.__encoders = iter(encoders)
        else:
            self.__encoders = None
        self.__current = self._next(None)

    def _next(self, current: Optional[Encoder]) -> Optional[Encoder]:
        try:
            return next(self.__encoders)
        except StopIteration:
            return None

    def encode(self, stream: BinaryIO) -> int:
        while self.__current is not None:
            if self.__current.has_remaining():
                return self.__current.encode(stream)
            self.__current = self._next(self.__current)

    def remaining(self) -> int:
        while self.__current is not None:
            if self.__current.has_remaining():
                return self.__current.remaining()
            self.__current = self._next(self.__current)
        return 0

    def has_remaining(self) -> bool:
        while self.__current is not None:
            if self.__current.has_remaining():
                return True
            self.__current = self._next(self.__current)
        return False


class MultipartDecoder(Decoder[_T]):
    def __init__(self, decoders: Optional[Iterable[Decoder]] = None):
        if decoders is not None:
            self.__decoders = iter(decoders)
        else:
            self.__decoders = None
        self.__current = None
        self.__state = 0

    def __next(self):
        if self.__state == 2:
            return None

        if self.__state == 0:
            self.__current = self._next(None)
            self.__state = 1
        elif self.__state == 1:
            self.__current = self._next(self.__current)

        if self.__current is None:
            self.__state = 2
            self._flush()

        return self.__current

    def _next(self, current: Optional[Decoder]) -> Optional[Decoder]:
        try:
            return next(self.__decoders)
        except StopIteration:
            return None

    def _flush(self):
        pass

    @abstractmethod
    def get(self) -> _T: ...

    def decode(self, stream: BinaryIO) -> int:
        while self.__state != 2:
            if self.__current is not None and self.__current.has_remaining():
                return self.__current.decode(stream)
            self.__current = self.__next()
        return 0

    def remaining(self) -> int:
        while self.__state != 2:
            if self.__current is not None and self.__current.has_remaining():
                return self.__current.remaining()
            self.__current = self.__next()
        return 0

    def has_remaining(self) -> bool:
        while self.__state != 2:
            if self.__current is not None and self.__current.has_remaining():
                return True
            self.__current = self.__next()
        return False


#


class RawEncoder(Encoder[_T]):
    def __init__(self, value: bytes):
        self.__data = value
        self.__pos = 0

    def encode(self, stream: BinaryIO) -> int:
        if self.__pos >= len(self.__data):
            return 0
        n = stream.write(self.__data[self.__pos:])
        if n > 0:
            self.__pos += n
        return n

    def remaining(self) -> int:
        return len(self.__data) - self.__pos


class RawDecoder(Decoder[_T]):
    def __init__(self, size: int):
        if size <= 0:
            raise ValueError()
        self.__data = bytes()
        self.__size = size

    def get(self) -> bytes:
        if self.has_remaining():
            raise ValueError()
        return self.__data

    def decode(self, stream: BinaryIO) -> int:
        pos = len(self.__data)
        if pos >= self.__size:
            return 0

        data = stream.read(self.__size - pos)
        if not data:
            return 0

        self.__data += data
        if len(self.__data) >= self.__size:
            self._flush()

    def _flush(self):
        pass

    def remaining(self) -> int:
        return self.__size - len(self.__data)


#


class VarintEncoder(RawEncoder[_T]):
    def __init__(self, value: int):
        if value < 0:
            raise ValueError()
        data = bytearray()
        while True:
            octet = value & 0x7F
            value = value >> 7
            if value:
                octet = octet | 0x80
            data.append(octet)
            if not value:
                break
        super().__init__(data)


class VarintDecoder(Decoder[_T]):
    def __init__(self):
        self.__value = 0
        self.__offset = 0
        self.__term = False

    def get(self) -> int:
        if self.has_remaining():
            raise ValueError()
        return self.__value

    def decode(self, stream: BinaryIO) -> int:
        if self.__term:
            return 0
        data = stream.read(1)
        if not data:
            return 0
        octet, = data
        self.__value |= (octet & 0x7F) << self.__offset
        self.__offset += 7
        if not (octet & 0x80):
            self.__term = True
            self._flush()

    def _flush(self):
        pass

    def remaining(self) -> int:
        return 0 if self.__term else 1
