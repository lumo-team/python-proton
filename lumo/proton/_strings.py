from typing import BinaryIO

from lumo.codecs import *
from ._basic import *

__all__ = 'Bytes', 'String'


#

class BytesEncoder(MultipartEncoder[bytes]):
    def __init__(self, value: bytes):
        length = VarintEncoder(len(value))
        encoder = RawEncoder(value)
        super().__init__((length, encoder))


class BytesDecoder(Decoder[bytes]):
    def __init__(self):
        self.__length = VarintDecoder()
        self.__decoder = None
        self.__pos = 0

    def __get_decoder(self) -> RawDecoder:
        if self.__decoder is None:
            length = self.__length.get()
            self.__decoder = RawDecoder(length)
        return self.__decoder

    def get(self) -> bytes:
        return self.__get_decoder().get()

    def decode(self, stream: BinaryIO) -> int:
        if self.__length.remaining():
            return self.__length.decode(stream)
        return self.__get_decoder().decode(stream)

    def remaining(self) -> int:
        if self.__length.has_remaining():
            return self.__length.remaining()
        return self.__get_decoder().remaining()


class Bytes(Codec[bytes]):
    def encoder(self, value: bytes) -> Encoder[bytes]:
        return BytesEncoder(value)

    def decoder(self) -> Decoder[bytes]:
        return BytesDecoder()


#

class StringEncoder(BytesEncoder):
    def __init__(self, value: str):
        super().__init__(value.encode('utf-8'))


class StringDecoder(BytesDecoder):
    def get(self) -> str:
        return super().get().decode('utf-8')


class String(Codec[str]):
    def encoder(self, value: str) -> Encoder[str]:
        return StringEncoder(value)

    def decoder(self) -> Decoder[str]:
        return StringDecoder()
