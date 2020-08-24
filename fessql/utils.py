#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-26 下午3:32
"""
import weakref
from collections import MutableMapping, MutableSequence
from typing import Dict, List, Union

__all__ = ("_verify_message", "gen_class_name", "Cached")


def _verify_message(src_message: Dict, message: Union[List, Dict]):
    """
    对用户提供的message进行校验
    Args:
        src_message: 默认提供的消息内容
        message: 指定的消息内容
    Returns:

    """
    src_message = dict(src_message)
    message = message if isinstance(message, MutableSequence) else [message]
    required_field = {"msg_code", "msg_zh", "msg_en"}

    for msg in message:
        if isinstance(msg, MutableMapping):
            if set(msg.keys()).intersection(required_field) == required_field and msg["msg_code"] in src_message:
                src_message[msg["msg_code"]].update(msg)
    return src_message


def gen_class_name(underline_name: str):
    """
    由下划线的名称变为驼峰的名称
    Args:
        underline_name
    Returns:

    """
    return "".join([name.capitalize() for name in underline_name.split("_")])


class _Cached(type):
    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cls.__cache = weakref.WeakValueDictionary()

    def __call__(cls, *args, **kwargs):
        cached_name = f"{args}{kwargs}"
        if cached_name in cls.__cache:
            return cls.__cache[cached_name]
        else:
            obj = super().__call__(*args, **kwargs)
            cls.__cache[cached_name] = obj  # 这里是弱引用不能直接赋值，否则会被垃圾回收期回收
            return obj


class Cached(metaclass=_Cached):
    """
    缓存类
    Args:
    Returns:

    """
    pass
