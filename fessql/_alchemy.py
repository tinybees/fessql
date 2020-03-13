#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/3/1 下午3:51
"""
from typing import Dict, List, MutableMapping, MutableSequence, Tuple, Union

import sqlalchemy as sa
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm.attributes import InstrumentedAttribute

from .err import FuncArgsError
from .utils import gen_class_name

__all__ = ("AlchemyMixIn",)


class AlchemyMixIn(object):
    """
    base alchemy
    """

    Model = declarative_base()

    # noinspection PyUnresolvedReferences
    def _verify_sanic_app(self, ):
        """
        校验APP类型是否正确

        暂时只支持sanic框架
        Args:

        Returns:

        """

        try:
            from sanic import Sanic
        except ImportError as e:
            raise ImportError(f"Sanic import error {e}.")
        else:
            if not isinstance(self.app, Sanic):
                raise FuncArgsError("app type must be Sanic.")

    def gen_model(self, model_cls: DeclarativeMeta, class_suffix: str = None, table_suffix: str = None,
                  field_mapping: Dict[str, str] = None, fields: Union[Tuple[str], List[str]] = None):
        """
        用于根据现有的model生成新的model类

        1.主要用于分表的查询和插入生成新的model,这时候生成的model和原有的model一致,主要是类名和表明不同.
        2.映射字段主要用来处理同一个字段在不同的库中有不同的名称的情况
        3.生成新的model类时的字段多少,如果字段比model_cls类中的多,则按照model_cls中的字段为准,
        如果字段比model_cls类中的少,则以fields中的为准
        Args:
            model_cls: 要生成分表的model类
            class_suffix: 新的model类名的后缀,生成新的类时需要使用
            table_suffix: 新的table名的后缀,生成新的表名时需要使用
            field_mapping: 字段映射,字段别名,如果有字段别名则生成的别名按照映射中的别名来,
                           如果没有则按照model_cls中的name来处理
            fields: 生成新的model类时的字段多少,如果字段比model_cls类中的多,则按照model_cls中的字段为准,
                    如果字段比model_cls类中的少,则以fields中的为准
        Returns:
            新生成的model类
        """
        if not issubclass(model_cls, self.Model):
            raise ValueError("model_cls must be db.Model type.")

        table_name = f"{getattr(model_cls, '__tablename__', model_cls.__name__.rstrip('Model'))}"
        if class_suffix:
            class_name = f"{gen_class_name(table_name)}{class_suffix.capitalize()}Model"
        else:
            class_name = f"{gen_class_name(table_name)}Model"
        if table_suffix:
            table_name = f"{table_name}_{table_suffix}"

        if getattr(model_cls, "_cache_class", None) is None:
            setattr(model_cls, "_cache_class", {})

        model_cls_ = getattr(model_cls, "_cache_class").get(class_name, None)
        if model_cls_ is None:
            model_fields = {}
            field_mapping = {} if not isinstance(field_mapping, MutableMapping) else field_mapping
            fields = tuple() if not isinstance(fields, MutableSequence) else (*fields, *field_mapping.keys())
            for attr_name, field in model_cls.__dict__.items():
                if isinstance(field, InstrumentedAttribute) and not attr_name.startswith("_"):
                    if fields and attr_name not in fields:
                        continue
                    model_fields[attr_name] = sa.Column(
                        name=field_mapping.get(attr_name, field.name),
                        type_=field.type, primary_key=field.primary_key, index=field.index,
                        nullable=field.nullable, default=field.default, onupdate=field.onupdate,
                        unique=field.unique, autoincrement=field.autoincrement, doc=field.doc)
            model_cls_ = type(class_name, (self.Model,), {
                "__doc__": model_cls.__doc__,
                "__table_args__ ": getattr(
                    model_cls, "__table_args__", None) or {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4'},
                "__tablename__": table_name,
                "__module__": model_cls.__module__,
                **model_fields})
            getattr(model_cls, "_cache_class")[class_name] = model_cls_

        return model_cls_
