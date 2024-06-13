#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2024/6/11 下午6:52


"""
from contextlib import contextmanager
from typing import Any, ClassVar, ContextManager, Dict, List, Optional, Sequence, Type, Union

from sqlalchemy import orm
# noinspection PyProtectedMember
from sqlalchemy.engine import Engine
from sqlalchemy.engine.result import RowProxy
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import DeclarativeMeta

from ._query import FesQuery
from .drivers import DialectDriver
from .._alchemy import AlchemyMixIn

__all__ = ("FesSession", "FesMgrSession", "DBAlchemy")


class FesSession(object):
    """
    改造orm的session类使得能够自动提示query的所有方法
    """
    bind_key: str
    is_closed: bool
    mgr_session: 'FesMgrSession'

    def __init__(self, autocommit: bool = False, autoflush: bool = True, expire_on_commit=False,
                 query_cls: Type[FesQuery] = FesQuery, **options): ...

    # noinspection PyTypeChecker
    def query(self, *entities, **kwargs) -> FesQuery: ...

    def close(self): ...


class FesMgrSession(object):
    """
    单个session的工厂管理类
    """
    _scoped_session: orm.scoped_session
    bind_key: Optional[str]

    def __init__(self, scoped_session: orm.scoped_session, bind_key: Optional[str] = None): ...

    def sessfes(self, ) -> FesSession: ...

    def query(self, *entities, **kwargs) -> FesQuery: ...

    def execute(self, query: Union[FesQuery, str], params: Dict[str, Any] = None
                ) -> Optional[RowProxy]: ...

    def query_execute(self, query: Union[FesQuery, str], params: Dict[str, Any] = None, size: int = None
                      ) -> Union[List[RowProxy], RowProxy, None]: ...


class DBAlchemy(AlchemyMixIn, object):
    """
    DB同步操作指南,基于SQlalchemy
    """
    Model: ClassVar[DeclarativeMeta]
    app: Optional[Any]
    # engine pool
    engine_pool: Dict[Optional[str], Engine]
    # session maker pool
    sessionmaker_pool: Dict[Optional[str], Union[orm.sessionmaker, orm.scoped_session]]
    dialect: str
    charset: str
    binary_prefix: bool
    # default bind connection
    username: str
    passwd: Optional[str]
    host: str
    port: int
    dbname: Optional[str]
    db_uri: URL
    # session and engine
    kwargs: Dict[str, Any]
    session_options: Dict[str, Any]
    engine_options: Dict[str, Any]
    # other binds
    fessql_binds: Dict[str, Dict]
    Query: Type[FesQuery]

    def __init__(self, app=None, *, username: str = "root", passwd: str = None,
                 host: str = "127.0.0.1", port: int = 3306, dbname: str = None,
                 dialect: str = DialectDriver.mysql_pymysql, fessql_binds: Dict[str, Dict] = None,
                 query_class: Type[FesQuery] = FesQuery, session_options: Dict[str, Any] = None,
                 engine_options: Dict[str, Any] = None, **kwargs): ...

    def _set_session_opts(self, ): ...

    def _set_engine_opts(self, ): ...

    @staticmethod
    def _apply_engine_opts(configs: Dict[str, Any], options: Dict[str, Any]): ...

    def get_engine_url(self, db_name, *, username: str = None, password: str = None, host="127.0.0.1",
                       port: int = 3306) -> URL: ...

    # noinspection DuplicatedCode
    def init_app(self, app): ...

    # noinspection DuplicatedCode
    def _init_app(self, config: Dict[str, Any]): ...

    # noinspection DuplicatedCode
    def init_engine(self, *, username: str = "root", passwd: str = None,
                    host: str = "127.0.0.1", port: int = 3306, dbname: str = None, **kwargs): ...

    def close_connection(self, ): ...

    def _create_scoped_sessionmaker(self, bind: Engine) -> orm.scoped_session: ...

    def _create_sessionmaker(self, bind: Engine) -> orm.sessionmaker: ...

    @staticmethod
    def _create_engine(sa_url: Union[str, URL], engine_opts: Dict[str, Any]) -> Engine: ...

    def _create_pool_engine(self, bind_key: str) -> None: ...

    def _gen_sessionmaker(self, bind_key: str = None) -> orm.scoped_session: ...

    def ping_session(self, session: FesSession, reconnect=True) -> FesSession: ...

    def gen_session(self, bind_key: Optional[str] = None) -> FesMgrSession: ...

    @property
    def session(self, ) -> FesMgrSession: ...

    @staticmethod
    @contextmanager
    def insert_context(session: FesMgrSession) -> ContextManager[FesSession]: ...

    @staticmethod
    @contextmanager
    def update_context(session: FesMgrSession) -> ContextManager[FesSession]: ...

    @staticmethod
    @contextmanager
    def delete_context(session: FesMgrSession) -> ContextManager[FesSession]: ...

    def verify_binds(self, ): ...

    def gen_model(self, model_cls: DeclarativeMeta, class_suffix: str = None, table_suffix: str = None,
                  table_name: str = None, field_mapping: Dict[str, str] = None,
                  fields: Sequence[str] = None): ...
