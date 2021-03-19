#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-25 下午4:58
"""
import threading
from collections import MutableMapping, MutableSequence
from contextlib import contextmanager
from typing import Dict, Generator, List, Union

import aelog
from sqlalchemy import exc as sqlalchemy_err, text
from sqlalchemy.engine.result import ResultProxy, RowProxy
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.orm import Query, Session
from sqlalchemy.orm.scoping import scoped_session

try:
    from flask import g
    from flask_sqlalchemy import BaseQuery, Pagination, SQLAlchemy
except ImportError as ex:
    # noinspection Mypy
    class FesSQLAlchemy:
        """
        抛出未import异常
        """

        def __init__(self) -> None:
            raise ImportError(f"flask_sqlalchemy import error {ex}.")


    class FesFlask:
        """
        抛出未import异常
        """

        def __setattr__(self, key, value) -> None:
            raise ImportError(f"Flask import error {ex}.")

        def __getattr__(self, item) -> None:
            raise ImportError(f"Flask import error {ex}.")


    BaseQuery = Pagination = SQLAlchemy = FesSQLAlchemy
    g = FesFlask

from .drivers import DialectDriver
from ._query import FesQuery
from .._alchemy import AlchemyMixIn
from .._cachelru import LRU
from .._err_msg import mysql_msg
from ..err import DBDuplicateKeyError, DBError, FuncArgsError, HttpError
from ..utils import _verify_message, Undefined

__all__ = ("FlaskAlchemy",)

_lru_cache = LRU()


class FlaskAlchemy(AlchemyMixIn, SQLAlchemy):
    """
    DB同步操作指南
    """

    def __init__(self, app=None, *, username: str = "root", passwd: str = None, host: str = "127.0.0.1",
                 port: int = 3306, dbname: str = None, pool_size: int = 10, is_binds: bool = False,
                 binds: Dict[str, str] = None, dialect: str = DialectDriver.mysql_pymysql, **kwargs):
        """
        DB同步操作指南
        Args:
            app: app应用
            host:mysql host
            port:mysql port
            dbname: database name
            username: mysql user
            passwd: mysql password
            pool_size: mysql pool size
            is_binds: Whether to bind same table different database, default false
            binds : Binds corresponds to  SQLALCHEMY_BINDS
            bind_func: Get the implementation logic of the bound value
            dialect: sqlalchemy默认的Dialect驱动
            kwargs: 其他参数 eg: charset,binary_prefix,pool_recycle
        """
        self.app_ = app
        self.username = username
        self.passwd = passwd
        self.host = host
        self.port = port
        self.dbname = dbname
        self.pool_size = pool_size
        self.is_binds = is_binds
        self.binds: Dict = binds or {}
        self.charset = kwargs.get("charset", "utf8mb4")
        self.binary_prefix = kwargs.get("binary_prefix", True)
        self.bind_func = kwargs.get("bind_func", None)
        self.message = kwargs.get("message", {})
        self.use_zh = kwargs.get("use_zh", True)
        self.pool_recycle = kwargs.get("pool_recycle", 3600)
        self.pool_recycle = self.pool_recycle if isinstance(self.pool_recycle, int) else 3600
        self.dialect: str = dialect
        self.msg_zh: str = ""
        self.scoped_sessions: Dict[str, scoped_session] = {}  # 主要保存其他scope session
        self.registry = threading.local()  # 当前线程注册bind key

        # 这里要用重写的BaseQuery, 根据BaseQuery的规则,Model中的query_class也需要重新指定为子类model,
        # 但是从Model的初始化看,如果Model的query_class为None的话还是会设置为和Query一致，符合要求
        super().__init__(app, query_class=FesQuery)

    def init_app(self, app, username: str = None, passwd: str = None, host: str = None, port: int = None,
                 dbname: str = None, pool_size: int = None, is_binds: bool = None,
                 binds: Dict[str, str] = None, **kwargs):
        """
        mysql 实例初始化

        Args:
            app: app应用
            host:mysql host
            port:mysql port
            dbname: database name
            username: mysql user
            passwd: mysql password
            pool_size: mysql pool size
            is_binds: Whether to bind same table different database, default false
            binds : Binds corresponds to  SQLALCHEMY_BINDS
            kwargs: 其他参数 eg: charset,binary_prefix,pool_recycle
        Returns:

        """
        self.app_ = app
        self.username = username or app.config.get("FESSQL_MYSQL_USERNAME") or self.username
        passwd = passwd or app.config.get("FESSQL_MYSQL_PASSWD") or self.passwd
        self.host = host or app.config.get("FESSQL_MYSQL_HOST") or self.host
        self.port = port or app.config.get("FESSQL_MYSQL_PORT") or self.port
        self.dbname = dbname or app.config.get("FESSQL_MYSQL_DBNAME") or self.dbname
        self.pool_size = pool_size or app.config.get("FESSQL_MYSQL_POOL_SIZE") or self.pool_size

        self.binds = binds or app.config.get("FESSQL_BINDS") or self.binds

        message = kwargs.get("message") or app.config.get("FESSQL_MYSQL_MESSAGE") or self.message
        use_zh = kwargs.get("use_zh") or app.config.get("FESSQL_MYSQL_MSGZH") or self.use_zh

        self.is_binds = is_binds or app.config.get("FESSQL_IS_BINDS") or self.is_binds
        self.bind_func = kwargs.get("bind_func") or self.bind_func

        self.pool_recycle = kwargs.get("pool_recycle") or app.config.get("FESSQL_POOL_RECYCLE") or self.pool_recycle
        self.charset = kwargs.get("charset") or self.charset
        self.binary_prefix = kwargs.get("binary_prefix") or self.binary_prefix

        self.passwd = passwd if passwd is None else str(passwd)
        self.message = _verify_message(mysql_msg, message)
        self.msg_zh = "msg_zh" if use_zh else "msg_en"

        app.config['SQLALCHEMY_DATABASE_URI'] = "{}://{}:{}@{}:{}/{}?charset={}&binary_prefix={}".format(
            self.dialect, self.username, self.passwd, self.host, self.port, self.dbname, self.charset,
            self.binary_prefix)
        app.config['SQLALCHEMY_BINDS'] = self.binds
        app.config['SQLALCHEMY_POOL_SIZE'] = self.pool_size
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        app.config['SQLALCHEMY_POOL_RECYCLE'] = self.pool_recycle

        # 如果绑定多个数据库标记为真，则初始化engine之前需要设置g的绑定数据库键，
        # 防止查询的是默认的SQLALCHEMY_DATABASE_URI
        # 这部分的具体逻辑交给具体的业务，通过实例的bind_func来实现
        if self.is_binds and self.bind_func and callable(self.bind_func):
            app.before_first_request_funcs.insert(0, self.bind_func)
        super().init_app(app)

        @app.teardown_appcontext
        def _shutdown_other_session(response_or_exc):
            for bind_key in getattr(self.registry, "bind_keys", set()):
                self.scoped_sessions[bind_key].remove()
            return response_or_exc

    def get_engine(self, app=None, bind=None):
        """Returns a specific engine."""
        # dynamic bind database
        # 如果model中指定了bind_key则，永远是指定的bind_key，即便g.bind_key指定了也是使用的model中的bind_key
        bind = g.bind_key if bind is None and self.is_binds and getattr(g, "bind_key", None) else bind
        return super().get_engine(app=app, bind=bind)

    def get_binds(self, app=None):
        """Returns a dictionary with a table->engine mapping.

        This is suitable for use of sessionmaker(binds=db.get_binds(app)).

        Increase the cache for table -> engine mapping

        bind_key is the bind mapping name,default None, that is SQLALCHEMY_DATABASE_URI
        """
        bind_name = g.bind_key if self.is_binds and getattr(g, "bind_key", None) else None

        if not _lru_cache.get(bind_name):
            _lru_cache[bind_name] = super().get_binds(app)
        return _lru_cache[bind_name]

    def ping_session(self, session: Union[Session, scoped_session] = None, reconnect=True
                     ) -> Union[scoped_session, Session]:
        """
        session探测,可以探测利用gen_session生成的session也可以探测默认的scope_session
        Args:
            session: session
            reconnect: 是否重连
        Returns:
            如果通则返回true,否则false
        """
        session = session if session else self.session
        try:
            session.execute(text("SELECT 1")).first()
        except sqlalchemy_err.OperationalError as err:
            if reconnect:
                if isinstance(session, Session):
                    bind_key = getattr(session, "bind_key", Undefined)
                    if bind_key != Undefined:
                        with self.set_bindkey(bind_key):
                            self.scoped_sessions[bind_key].remove()
                            session = self.scoped_sessions[bind_key]()
                            session.bind_key = bind_key  # 设置bind key
                    else:
                        raise FuncArgsError(f"session中缺少bind_key变量") from err
                else:
                    session.remove()
            else:
                raise err
        # 返回重建后的session
        return session

    def gen_session(self, bind_key: str, session_options: Dict = None) -> Session:
        """
        创建或者获取指定的session,这里是session非sessionmaker

        主要用于在一个视图内部针对同表不同库的数据请求获取
        Args:
            bind_key: session需要绑定的ECLIENTS_BINDS中的键
            session_options: create_session 所需要的字典或者关键字参数
        Returns:

        """
        if bind_key not in self.app_.config['SQLALCHEMY_BINDS']:
            raise ValueError(f"{bind_key} not in SQLALCHEMY_BINDS, please config it.")

        with self.set_bindkey(bind_key):
            if bind_key not in self.scoped_sessions:
                self.scoped_sessions[bind_key] = self.create_scoped_session(session_options)
            session = self.scoped_sessions[bind_key]()
            session.bind_key = bind_key  # 设置bind key
            session = self.ping_session(session)  # 校验重连,保证可用
            # 加入当前线程bindkey,用于自动关闭处理
            if hasattr(self.registry, "bind_keys") is False:
                self.registry.bind_keys = set()
            self.registry.bind_keys.add(bind_key)

        return session

    @contextmanager
    def set_bindkey(self, bind_key):
        """
        更新bind key的上下文,用于session的创建和ping功能
        Args:
            bind_key
        Returns:

        """
        # 为了保证不改变原始的默认session,这里创建新的session后需要还原
        src_bind_key = getattr(g, "bind_key", None)
        try:
            g.bind_key = bind_key  # 设置要切换的bind
            yield
        finally:
            g.bind_key = src_bind_key  # 还原

    @contextmanager
    def gsession(self, bind_key: str, session_options: Dict = None) -> Generator[Session, None, None]:
        """
        增加session的上下文自动关闭
        Args:
            bind_key: session需要绑定的ECLIENTS_BINDS中的键
            session_options: create_session 所需要的字典或者关键字参数
        Returns:

        """
        session: Session = self.gen_session(bind_key, session_options)
        try:
            yield session
        finally:
            session.close()

    def save(self, model_obj, session: Session = None):
        """
        保存model对象
        Args:
            model_obj: model对象
            session: session对象, 默认是self.session
        Returns:

        """
        self.session.add(model_obj) if session is None else session.add(model_obj)

    def save_all(self, model_objs: MutableSequence, session: Session = None):
        """
        保存model对象
        Args:
            model_objs: model对象
            session: session对象, 默认是self.session
        Returns:

        """
        if not isinstance(model_objs, MutableSequence):
            raise ValueError(f"model_objs应该是MutableSequence类型的")
        self.session.add_all(model_objs) if session is None else session.add_all(model_objs)

    def delete(self, model_obj, session: Session = None):
        """
        删除model对象
        Args:
            model_obj: model对象
            session: session对象, 默认是self.session
        Returns:

        """
        self.session.delete(model_obj) if session is None else session.delete(model_obj)

    @contextmanager
    def insert_context(self, session: Session = None) -> Generator['FlaskAlchemy', None, None]:
        """
        插入数据context
        Args:
            session: session对象, 默认是self.session
        Returns:

        """
        session = self.session if session is None else session
        try:
            yield self
            session.commit()
        except IntegrityError as e:
            session.rollback()
            if "Duplicate" in str(e):
                raise DBDuplicateKeyError(e)
            else:
                raise DBError(e)
        except DatabaseError as e:
            session.rollback()
            aelog.exception(e)
            raise DBError(e)
        except Exception as e:
            session.rollback()
            aelog.exception(e)
            raise HttpError(400, message=self.message[1][self.msg_zh], error=e)

    @contextmanager
    def update_context(self, session: Session = None) -> Generator['FlaskAlchemy', None, None]:
        """
        更新数据context
        Args:
            session: session对象, 默认是self.session
        Returns:

        """
        session = self.session if session is None else session
        try:
            yield self
            session.commit()
        except IntegrityError as e:
            session.rollback()
            if "Duplicate" in str(e):
                raise DBDuplicateKeyError(e)
            else:
                raise DBError(e)
        except DatabaseError as e:
            session.rollback()
            aelog.exception(e)
            raise DBError(e)
        except Exception as e:
            session.rollback()
            aelog.exception(e)
            raise HttpError(400, message=self.message[2][self.msg_zh], error=e)

    @contextmanager
    def delete_context(self, session: Session = None) -> Generator['FlaskAlchemy', None, None]:
        """
        删除数据context
        Args:
            session: session对象, 默认是self.session
        Returns:

        """
        session = self.session if session is None else session
        try:
            yield self
            session.commit()
        except DatabaseError as e:
            session.rollback()
            aelog.exception(e)
            raise DBError(e)
        except Exception as e:
            session.rollback()
            aelog.exception(e)
            raise HttpError(400, message=self.message[3][self.msg_zh], error=e)

    def _execute(self, query: Union[Query, str], params: Dict = None, session: Session = None) -> ResultProxy:
        """
        插入数据，更新或者删除数据
        Args:
            query: SQL的查询字符串或者sqlalchemy表达式
            params: SQL表达式中的参数
            session: session对象, 默认是self.session
        Returns:
            不确定执行的是什么查询，直接返回ResultProxy实例
        """
        session = self.session if session is None else session
        try:
            cursor = session.execute(query, params)
            session.commit()
        except IntegrityError as e:
            session.rollback()
            if "Duplicate" in str(e):
                raise DBDuplicateKeyError(e)
            else:
                raise DBError(e)
        except DatabaseError as e:
            session.rollback()
            aelog.exception(e)
            raise DBError(e)
        except Exception as e:
            session.rollback()
            aelog.exception(e)
            raise HttpError(400, message=self.message[2][self.msg_zh], error=e)
        else:
            return cursor

    # noinspection DuplicatedCode
    def execute(self, query: Union[Query, str], params: Dict = None, session: Session = None, size: int = None,
                cursor_close: bool = True) -> Union[List[RowProxy], RowProxy, None]:
        """
        插入数据，更新或者删除数据
        Args:
            query: SQL的查询字符串或者sqlalchemy表达式
            params: SQL表达式中的参数
            session: session对象, 默认是self.session
            size: 查询数据大小, 默认返回所有
            cursor_close: 是否关闭游标，默认关闭，如果多次读取可以改为false，后面关闭的行为交给sqlalchemy处理
        Returns:
            List[RowProxy] or RowProxy or None
        """
        params = dict(params) if isinstance(params, MutableMapping) else {}
        cursor = self._execute(query, params, session)
        if size is None:
            resp = cursor.fetchall() if cursor.returns_rows else []
        elif size == 1:
            resp = cursor.fetchone() if cursor.returns_rows else None
        else:
            resp = cursor.fetchmany(size) if cursor.returns_rows else []

        if cursor_close is True:
            cursor.close()

        return resp
