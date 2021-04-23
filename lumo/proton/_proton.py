import enum
import typing
from typing import Optional, ForwardRef, get_origin, get_args

from lumo.codecs import *
from lumo.types import Serializable, eval_type
from ._collections import *
from ._generics import *
from ._primitives import *
from ._strings import *

__all__ = 'Proton',


class Proton(CodecRegistry):
    def __init__(self):
        self.__codecs: typing.Dict[type, Codec] = {
            type(None): Null(),
            int: Integer(),
            float: Float(),
            bool: Boolean(),
            str: String(),
            bytes: Bytes(),
            bytearray: Bytes(),
        }
        self.__cache: typing.Dict[type, Codec] = {}

    def __resolve(
            self,
            descriptor: type,
            context: CodecContext
    ) -> typing.Optional[Codec]:
        if isinstance(descriptor, type):
            if issubclass(descriptor, Serializable):
                codec = Object.__new__(Object)
                self.__cache[descriptor] = codec
                try:
                    codecs = {}
                    for key, value in descriptor.__fields__.items():
                        value = context.codec(eval_type(value, descriptor))
                        if value is None:
                            del self.__cache[descriptor]
                            return None
                        codecs[key] = value
                    codec.__init__(descriptor, codecs)
                except Exception:
                    del self.__cache[descriptor]
                    raise
                return codec

            if issubclass(descriptor, enum.Enum):
                return Enum(descriptor)

        origin = get_origin(descriptor)
        if origin is None:
            return None

        args = get_args(descriptor)

        if origin in (list, set):
            arg, = args
            codec = context.codec(eval_type(arg, descriptor))
            return Collection(origin, codec) if codec else None

        if origin is dict:
            key_type, value_type = args
            key_codec = context.codec(eval_type(key_type, descriptor))
            if key_type is None:
                return None
            value_codec = context.codec(eval_type(value_type, descriptor))
            if value_type is None:
                return None
            return Dict(key_codec, value_codec)

        if origin is tuple:
            if args[-1] is Ellipsis:
                if len(args) != 2:
                    raise ValueError()
                arg = args[0]
                codec = context.codec(eval_type(arg, descriptor))
                return Collection(tuple, codec) if codec else None
            else:
                codecs = []
                for arg in args:
                    codec = context.codec(eval_type(arg, descriptor))
                    if codec is None:
                        return None
                    codecs.append(codec)
                return Tuple(codecs)

        if origin is typing.Union:
            codecs = []
            for arg in args:
                codec = context.codec(eval_type(arg, descriptor))
                if codec is None:
                    return None
                codecs.append((arg, codec))
            return Union(codecs)

        return None

    def codec(
            self,
            descriptor: typing.Union[type, ForwardRef, str],
            context: Optional[CodecContext] = None
    ) -> Optional[Codec]:
        if context is None:
            context = CodecContext(self)

        descriptor = eval_type(descriptor)
        if descriptor in self.__codecs:
            return self.__codecs[descriptor]
        if descriptor in self.__cache:
            return self.__cache[descriptor]

        codec = self.__resolve(descriptor, context)
        if codec is not None:
            self.__cache[descriptor] = codec

        return codec

    def register(self, descriptor: type, codec: Codec):
        self.__codecs[descriptor] = codec

    def unregister(self, descriptor: type, codec: Codec):
        if descriptor in self.__codecs and self.__codecs[descriptor] == codec:
            del self.__codecs[descriptor]
