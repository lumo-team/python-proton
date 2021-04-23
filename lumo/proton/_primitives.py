from struct import pack, unpack
from typing import BinaryIO

from lumo.codecs import *
from ._basic import *

__all__ = 'Null', 'Integer', 'Float', 'Boolean',


#


class NullEncoder(Encoder[None]):
    def encode(self, stream: BinaryIO) -> int:
        return 0

    def remaining(self) -> int:
        return 0


class NullDecoder(Decoder[None]):
    def get(self) -> None:
        return None

    def decode(self, stream: BinaryIO) -> int:
        return 0

    def remaining(self) -> int:
        return 0


class Null(Codec[None]):
    def encoder(self, value: None) -> Encoder[None]:
        return NullEncoder()

    def decoder(self) -> Decoder[None]:
        return NullDecoder()


#


class IntegerEncoder(VarintEncoder[int]):
    def __init__(self, value: int):
        value = int(value)
        value = (value << 1) ^ (value >> 31)
        super().__init__(value)


class IntegerDecoder(VarintDecoder[int]):
    def __init__(self):
        super().__init__()
        self.__value = None

    def _flush(self):
        value = super().get()
        signed = value & 1
        value = value >> 1
        if signed:
            value = ~value
        self.__value = value

    def get(self) -> int:
        if self.__value is None:
            raise ValueError()
        return self.__value


class Integer(Codec[int]):
    def encoder(self, value: int) -> Encoder[int]:
        return IntegerEncoder(value)

    def decoder(self) -> Decoder[int]:
        return IntegerDecoder()


#


class FloatEncoder(RawEncoder[float]):
    def __init__(self, value: float):
        value = float(value)
        super().__init__(pack('>f', value))


class FloatDecoder(RawDecoder[float]):
    def __init__(self):
        super().__init__(4)
        self.__value = None

    def _flush(self):
        self.__value, = unpack('>f', super().get())

    def get(self) -> float:
        if self.__value is None:
            raise ValueError()
        return self.__value


class Float(Codec[float]):
    def encoder(self, value: float) -> Encoder[float]:
        return FloatEncoder(value)

    def decoder(self) -> Decoder[float]:
        return FloatDecoder()


#


class BooleanEncoder(RawEncoder[bool]):
    def __init__(self, value: bool):
        value = bytes((1 if value else 0,))
        super().__init__(value)


class BooleanDecoder(RawDecoder[bool]):
    def __init__(self):
        super().__init__(1)
        self.__value = None

    def _flush(self):
        value, = super().get()
        if value == 0:
            self.__value = False
        elif value == 1:
            self.__value = True
        else:
            msg = f'Invalid boolean value 0x{value:02x}'
            raise DecoderException(msg)

    def get(self) -> bool:
        if self.__value is None:
            raise ValueError()
        return self.__value


class Boolean(Codec[bool]):
    def encoder(self, value: bool) -> Encoder[bool]:
        return BooleanEncoder(value)

    def decoder(self) -> Decoder[bool]:
        return BooleanDecoder()
