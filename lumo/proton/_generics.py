import enum
import typing
from typing import Any, Sequence, Dict, Type, BinaryIO, Optional
from typing import TypeVar

from lumo.codecs import *
from lumo.types import Serializable
from ._basic import *

__all__ = 'Enum', 'Union', 'Tuple', 'Object'

#
from ._basic import _T

_Enum = TypeVar('_Enum', bound=enum.Enum)


class EnumEncoder(VarintEncoder[_Enum]):
    def __init__(self, value: _Enum, members: Sequence[_Enum]):
        super().__init__(members.index(value))


class EnumDecoder(VarintDecoder[_Enum]):
    def __init__(self, members: Sequence[_Enum]):
        self.__members = members
        self.__value = None
        super().__init__()

    def _flush(self):
        index = super().get()
        if index > len(self.__members):
            raise EncoderException(f'Invalid enum value {index}')
        self.__value = self.__members[index]

    def get(self) -> _Enum:
        if self.__value is None:
            raise ValueError()
        return self.__value


class Enum(Codec[_Enum]):
    def __init__(self, type: Type[_Enum]):
        self.__members = list(type.__members__.values())

    def encoder(self, value: _Enum) -> Encoder[_Enum]:
        return EnumEncoder(value, self.__members)

    def decoder(self) -> Decoder[_Enum]:
        return EnumDecoder(self.__members)


#


class UnionEncoder(MultipartEncoder):
    def __init__(self, value, choices: Sequence[typing.Tuple[type, Codec]]):
        index = None
        match = None

        for i, (choice, codec) in enumerate(choices):
            if isinstance(choice, type) and isinstance(value, choice):
                if match is None or match in choice.mro():
                    match = choice
                    index = i
            elif value is choice:
                match = choice
                index = i
                break

        if match is None:
            raise ValueError()

        choice, codec = choices[index]
        encoders = [VarintEncoder(index), codec.encoder(value)]
        super().__init__(encoders)


class UnionDecoder(MultipartDecoder):
    def __init__(self, choices: Sequence[typing.Tuple[type, Codec]]):
        self.__choices = tuple(choices)
        self.__decoder = None
        super().__init__()

    def _next(self, current: Optional[Decoder]) -> Optional[Decoder]:
        if current is None:
            return VarintDecoder()

        if self.__decoder is None:
            index = current.get()
            if index > len(self.__choices):
                msg = f'Invalid type index {index}'
                raise DecoderException(msg)
            type, codec = self.__choices[index]
            decoder = codec.decoder()
            self.__decoder = decoder
            return decoder

        return None

    def get(self) -> _T:
        if self.__decoder is None:
            raise ValueError()
        return self.__decoder.get()


class Union(Codec):
    def __init__(self, choices: Sequence[typing.Tuple[type, Codec]]):
        self.__choices = tuple(choices)

    def encoder(self, value) -> Encoder:
        return UnionEncoder(value, self.__choices)

    def decoder(self) -> Decoder:
        return UnionDecoder(self.__choices)


#


class TupleEncoder(MultipartEncoder):
    def __init__(self, values: tuple, codecs: Sequence[Codec]):
        if len(values) != len(codecs):
            raise ValueError()
        self.__values = zip(values, codecs)
        super().__init__()

    def _next(self, current: Optional[Encoder]) -> Optional[Encoder]:
        try:
            value, codec = next(self.__values)
        except StopIteration:
            return None
        return codec.encoder(value)


class TupleDecoder(MultipartDecoder):
    def __init__(self, codecs: Sequence[Codec]):
        self.__codecs = iter(codecs)
        self.__items = []
        self.__size = len(codecs)
        super().__init__()

    def _next(self, current: Optional[Decoder]) -> Optional[Decoder]:
        if current is not None:
            self.__items.append(current.get())
        try:
            codec = next(self.__codecs)
        except StopIteration:
            return None
        return codec.decoder()

    def _flush(self):
        self.__items = tuple(self.__items)

    def get(self) -> tuple:
        if len(self.__items) < self.__size:
            raise ValueError()
        return self.__items


class Tuple(Codec[tuple]):
    def __init__(self, codecs: Sequence[Codec]):
        self.__codecs = tuple(codecs)

    def encoder(self, values: tuple) -> Encoder[tuple]:
        return TupleEncoder(values, self.__codecs)

    def decoder(self) -> Decoder[tuple]:
        return TupleDecoder(self.__codecs)


#


_Serializable = TypeVar('_Serializable', bound=Serializable)


class ObjectEncoder(MultipartEncoder[_Serializable]):
    def __init__(self, value: _Serializable, type: Type[_Serializable], codecs: Dict[str, Codec]):
        if not isinstance(value, type):
            raise ValueError()
        values = value.dump()
        values = tuple((values[key], codec) for key, codec in codecs.items())
        self.__values = iter(values)
        super().__init__()

    def _next(self, current: Optional[Encoder]) -> Optional[Encoder]:
        try:
            value, codec = next(self.__values)
        except StopIteration:
            return None
        return codec.encoder(value)


class ObjectDecoder(MultipartDecoder[_Serializable]):
    def __init__(self, type: Type[_Serializable], codecs: Dict[str, Codec]):
        codecs = dict(codecs)
        self.__type = type
        self.__keys = iter(codecs.keys())
        self.__codecs = iter(codecs.values())
        self.__items = {}
        self.__size = len(codecs)
        super().__init__()

    def _next(self, current: Optional[Decoder]) -> Optional[Decoder]:
        if current is not None:
            self.__items[next(self.__keys)] = current.get()
        try:
            codec = next(self.__codecs)
        except StopIteration:
            return None
        return codec.decoder()

    def get(self) -> _Serializable:
        if len(self.__items) < self.__size:
            raise ValueError()
        return self.__type.load(self.__items)


class Object(Codec[_Serializable]):
    def __init__(self, type: Type[_Serializable], codecs: Dict[str, Codec]):
        self.__type = type
        self.__codecs = codecs

    def encoder(self, value: _Serializable) -> Encoder[_Serializable]:
        return ObjectEncoder(value, self.__type, self.__codecs)

    def decoder(self) -> Decoder[_Serializable]:
        return ObjectDecoder(self.__type, self.__codecs)
