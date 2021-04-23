import typing
from typing import Callable, Iterator, Optional
from typing import TypeVar

from lumo.codecs import *
from ._basic import *
from ._generics import Tuple

__all__ = 'Collection', 'Dict',

#

_Collection = TypeVar('_Collection', bound=typing.Collection)


class CollectionEncoder(MultipartEncoder[_Collection]):
    def __init__(self, values: _Collection, codec: Codec):
        self.__size = len(values)
        self.__values = iter(values)
        self.__codec = codec
        super().__init__()

    def _next(self, current: Optional[Encoder]) -> Optional[Encoder]:
        if current is None:
            return VarintEncoder(self.__size)
        try:
            value = next(self.__values)
        except StopIteration:
            return None
        return self.__codec.encoder(value)


class CollectionDecoder(MultipartDecoder[_Collection]):
    def __init__(self, constructor: Callable[[Iterator], _Collection], codec: Codec):
        self.__ctor = constructor
        self.__codec = codec
        self.__items = []
        self.__size = None
        super().__init__()

    def _next(self, current: Optional[Decoder]) -> Optional[Decoder]:
        if current is None:
            return VarintDecoder()

        if self.__size is None:
            self.__size = current.get()
        else:
            self.__items.append(current.get())

        if len(self.__items) >= self.__size:
            return None

        decoder = self.__codec.decoder()
        return decoder

    def _flush(self):
        self.__items = tuple(self.__items)

    def get(self) -> _Collection:
        if self.__size is None or len(self.__items) < self.__size:
            raise ValueError()
        return self.__ctor(self.__items)


class Collection(Codec[_Collection]):
    def __init__(self, constructor: Callable[[Iterator], _Collection], codec: Codec):
        self.__ctor = constructor
        self.__codec = codec

    def encoder(self, value: _Collection) -> Encoder[_Collection]:
        return CollectionEncoder(value, self.__codec)

    def decoder(self) -> Decoder[_Collection]:
        return CollectionDecoder(self.__ctor, self.__codec)


#

_K = TypeVar('_K')
_V = TypeVar('_V')


class DictEncoder(CollectionEncoder[typing.Dict[_K, _V]]):
    def __init__(self, value: typing.Dict[_K, _V], key_codec: Codec[_K], value_codec: Codec[_V]):
        items = list(value.items())
        codec = Tuple((key_codec, value_codec))
        super().__init__(items, codec)


class DictDecoder(CollectionDecoder[typing.Dict[_K, _V]]):
    def __init__(self, key_codec: Codec[_K], value_codec: Codec[_V]):
        codec = Tuple((key_codec, value_codec))
        super().__init__(dict, codec)


class Dict(Codec[typing.Dict[_K, _V]]):
    def __init__(self, key: Codec[_K], value: Codec[_V]):
        self.__key = key
        self.__value = value

    def encoder(self, value: typing.Dict[_K, _V]) -> Encoder[typing.Dict[_K, _V]]:
        return DictEncoder(value, self.__key, self.__value)

    def decoder(self) -> Decoder[typing.Dict[_K, _V]]:
        return DictDecoder(self.__key, self.__value)
