#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/11/10 下午4:14
"""
import atexit
from collections import MutableMapping
from contextlib import contextmanager
from math import ceil
from typing import Any, Dict, Generator, List, Optional, Type, Union

import aelog
import sqlalchemy
from sqlalchemy import exc as sqlalchemy_err, orm, text
# noinspection PyProtectedMember
from sqlalchemy.engine import Engine
from sqlalchemy.engine.result import ResultProxy, RowProxy
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.sql.schema import Table

from .drivers import DialectDriver
from .._alchemy import AlchemyMixIn
from .._err_msg import mysql_msg
from ..err import DBDuplicateKeyError, DBError, FuncArgsError, HttpError

__all__ = ("FesPagination", "FesQuery", "FesSession", "FastapiAlchemy")


class FesPagination(object):
    """Internal helper class returned by :meth:`BaseQuery.paginate`.  You
    can also construct it from any other SQLAlchemy query object if you are
    working with other libraries.  Additionally it is possible to pass `None`
    as query object in which case the :meth:`prev` and :meth:`next` will
    no longer work.
    """

    def __init__(self, query: 'FesQuery', page: int, per_page: int, total: int, items: List[RowProxy]):
        #: the unlimited query object that was used to create this
        #: pagination object.
        self.query: FesQuery = query
        #: the current page number (1 indexed)
        self.page: int = page
        #: the number of items to be displayed on a page.
        self.per_page: int = per_page
        #: the total number of items matching the query
        self.total: int = total
        #: the items for the current page
        self.items: List[RowProxy] = items

    @property
    def pages(self):
        """The total number of pages"""
        if self.per_page == 0 or self.total is None:
            pages = 0
        else:
            pages = int(ceil(self.total / float(self.per_page)))
        return pages

    def prev(self, error_out=False):
        """Returns a :class:`Pagination` object for the previous page."""
        assert (
                self.query is not None
        ), "a query object is required for this method to work"
        return self.query.paginate(self.page - 1, self.per_page, error_out)

    @property
    def prev_num(self):
        """Number of the previous page."""
        if not self.has_prev:
            return None
        return self.page - 1

    @property
    def has_prev(self):
        """True if a previous page exists"""
        return self.page > 1

    def next(self, error_out=False):
        """Returns a :class:`Pagination` object for the next page."""
        assert (
                self.query is not None
        ), "a query object is required for this method to work"
        return self.query.paginate(self.page + 1, self.per_page, error_out)

    @property
    def has_next(self):
        """True if a next page exists."""
        return self.page < self.pages

    @property
    def next_num(self):
        """Number of the next page"""
        if not self.has_next:
            return None
        return self.page + 1


class FesQuery(orm.Query):
    """
    改造Query,使得符合业务中使用

    目前是改造如果limit传递为0，则返回所有的数据，这样业务代码中就不用更改了
    """

    # noinspection DuplicatedCode
    def paginate(self, page: int = 1, per_page: int = 20, max_per_page: int = None,
                 primary_order: bool = True) -> FesPagination:
        """Returns ``per_page`` items from page ``page``.

        If ``page`` or ``per_page`` are ``None``, they will be retrieved from
        the request query. If ``max_per_page`` is specified, ``per_page`` will
        be limited to that value. If there is no request or they aren't in the
        query, they default to 1 and 20 respectively.

        * No items are found and ``page`` is not 1.
        * ``page`` is less than 1, or ``per_page`` is negative.
        * ``page`` or ``per_page`` are not ints.
        * primary_order: 默认启用主键ID排序的功能，在大数据查询时可以关闭此功能，在90%数据量不大的情况下可以加快分页的速度

        ``page`` and ``per_page`` default to 1 and 20 respectively.

        Returns a :class:`Pagination` object.
        """

        try:
            page = int(page)
        except (TypeError, ValueError):
            page = 1

        try:
            per_page = int(per_page)
        except (TypeError, ValueError):
            per_page = 20

        if max_per_page is not None:
            per_page = min(per_page, max_per_page)

        if page < 1:
            page = 1

        if per_page < 0:
            per_page = 20

        if primary_order is True:

            # 如果分页获取的时候没有进行排序,并且model中有id字段,则增加用id字段的升序排序
            # 前提是默认id是主键,因为不排序会有混乱数据,所以从中间件直接解决,业务层不需要关心了
            # 如果业务层有排序了，则此处不再提供排序功能
            # 如果遇到大数据量的分页查询问题时，建议关闭此处，然后再基于已有的索引分页
            if self._order_by is False or self._order_by is None:  # type: ignore
                select_model = getattr(self._primary_entity, "selectable", None)

                if isinstance(select_model, Table) and getattr(select_model.columns, "id", None) is not None:
                    self._order_by = [select_model.columns.id.asc()]

        # 如果per_page为0,则证明要获取所有的数据，否则还是通常的逻辑
        if per_page != 0:
            items = self.limit(per_page).offset((page - 1) * per_page).all()
        else:
            items = self.all()

        # No need to count if we're on the first page and there are fewer
        # items than we expected.
        if page == 1 and len(items) < per_page:
            total = len(items)
        else:
            total = self.order_by(None).count()

        return FesPagination(self, page, per_page, total, items)

    def filter(self, *criterion) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().filter(*criterion)

    def filter_by(self, **kwargs) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().filter_by(**kwargs)

    def with_entities(self, *entities) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().with_entities(*entities)

    def options(self, *args) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().options(*args)

    def with_session(self, session) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().with_session(session)

    def order_by(self, *criterion) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().order_by(*criterion)

    def group_by(self, *criterion) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().group_by(*criterion)

    def having(self, criterion) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().having(criterion)

    def union(self, *q) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().union(*q)

    def union_all(self, *q) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().union_all(*q)

    def distinct(self, *expr) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().distinct(*expr)

    def with_hint(self, selectable, text_, dialect_name="*") -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().with_hint(selectable, text_, dialect_name)

    def execution_options(self, **kwargs) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().execution_options(**kwargs)


class FesSession(orm.Session):
    """
    改造orm的session类使得能够自动提示query的所有方法
    """

    def __init__(self, autocommit: bool = False, autoflush: bool = True,
                 query_cls: Type[FesQuery] = FesQuery, **options):
        """
            改造orm的session类使得能够自动提示query的所有方法
        Args:

        """
        super().__init__(autocommit=autocommit, autoflush=autoflush, query_cls=query_cls, **options)

    # noinspection PyTypeChecker
    def query(self, *entities, **kwargs) -> FesQuery:
        """Return a new :class:`.FesQuery` object corresponding to this
        :class:`.Session`."""

        return super().query(*entities, **kwargs)


class FastapiAlchemy(AlchemyMixIn, object):
    """
    DB同步操作指南，适用于fastapi
    """

    def __init__(self, app=None, *, username: str = "root", passwd: str = None,
                 host: str = "127.0.0.1", port: int = 3306, dbname: str = None,
                 dialect: str = DialectDriver.mysql_pymysql, fessql_binds: Dict[str, Dict] = None,
                 query_class: Type[FesQuery] = FesQuery, session_options: Dict[str, Any] = None,
                 engine_options: Dict[str, Any] = None, **kwargs):
        """
        DB同步操作指南，适用于fastapi
        Args:
            app: app应用
            username: mysql user
            passwd: mysql password
            host:mysql host
            port:mysql port
            dbname: database name
            dialect: sqlalchemy默认的Dialect驱动
            fessql_binds: fesql binds
            query_class: 查询类,orm.Query的子类
            session_options: 创建session的关键字参数
            engine_options: 创建engine的关键字参数

            autoflush: 是否自动flush,默认True
            autocommit: 是否自动commit,默认false

            pool_size: mysql pool size
            pool_recycle: pool recycle time, type int
            pool_timeout: 连接池超时时间,默认60秒
            pool_use_lifo: 是否后进先出,默认True
            max_overflow: 最大溢出连接数,默认10
            echo: 是否显示sqlalchemy的日志,默认false
            connect_args: 实际建立连接的连接参数,connect_timeout: 连接超时时间，默认60秒

            fessql_binds: binds config, eg:{"first":{"fessql_mysql_host":"127.0.0.1",
                                                    "fessql_mysql_port":3306,
                                                    "fessql_mysql_username":"root",
                                                    "fessql_mysql_passwd":"",
                                                    "fessql_mysql_dbname":"dbname"}}
        """
        self.app = app
        # engine pool
        self.engine_pool: Dict[Optional[str], Engine] = {}
        # session maker pool
        self.sessionmaker_pool: Dict[Optional[str], Union[orm.sessionmaker, orm.scoped_session]] = {}
        self.dialect: str = dialect
        self.charset: str = "utf8mb4"  # 连接编码
        self.binary_prefix: bool = kwargs.get("binary_prefix", False)  # 是否增加二进制前缀连接URL中
        # default bind connection
        self.username: str = username
        self.passwd: Optional[str] = passwd
        self.host: str = host
        self.port: int = port
        self.dbname: Optional[str] = dbname
        self.db_uri: str = ""
        # session and engine
        self.kwargs: Dict[str, Any] = kwargs
        self.session_options: Dict[str, Any] = session_options or {}
        self._set_session_opts()  # 更新默认参数
        self.engine_options: Dict[str, Any] = engine_options or {}
        self._set_engine_opts()  # 更新默认参数
        # other binds
        self.fessql_binds: Dict[str, Dict] = fessql_binds or {}  # binds config
        self.Query = query_class

        if app is not None:
            self.init_app(app)

    def _set_session_opts(self, ):
        """
        设置创建session的默认值
        Args:

        Returns:

        """
        self.session_options.setdefault("autoflush", self.kwargs.get("autoflush", True))
        self.session_options.setdefault("autocommit", self.kwargs.get("autocommit", False))

    def _set_engine_opts(self, ):
        """
        设置创建引擎的默认值
        Args:

        Returns:

        """
        self.engine_options.setdefault("pool_size", self.kwargs.get("pool_size", 10))
        self.engine_options.setdefault("pool_recycle", self.kwargs.get("pool_recycle", 3600))
        self.engine_options.setdefault("pool_timeout", self.kwargs.get("pool_timeout", 60))
        self.engine_options.setdefault("max_overflow", self.kwargs.get("max_overflow", 10))
        self.engine_options.setdefault("pool_use_lifo", self.kwargs.get("pool_use_lifo", True))
        self.engine_options.setdefault("echo", self.kwargs.get("echo", False))
        self.engine_options.setdefault("connect_args", {
            "connect_timeout": self.kwargs.get("connect_timeout", 60),
            "charset": self.charset, "binary_prefix": self.binary_prefix})

    @staticmethod
    def apply_engine_opts(configs: Dict[str, Any], options: Dict[str, Any]):
        """
        应用从配置读取的引擎参数
        Args:
            configs: 配置参数
            options: 引擎参数
        Returns:

        """

        def _setdefault(optionkey, configkey):
            value = configs.get(configkey)
            if value is not None:
                options[optionkey] = value

        _setdefault('pool_size', 'FESSQL_POOL_SIZE')
        _setdefault('pool_recycle', 'FESSQL_POOL_RECYCLE')
        _setdefault('pool_timeout', 'FESSQL_POOL_TIMEOUT')
        _setdefault('max_overflow', 'FESSQL_MAX_OVERFLOW')
        _setdefault('pool_use_lifo', 'FESSQL_POOL_USE_LIFO')
        _setdefault('echo', 'FESSQL_ECHO')
        _setdefault('connect_args', 'FESSQL_CONNECT_ARGS')

    def get_engine_url(self, db_name, *, username: str = None, password: str = None, host="127.0.0.1",
                       port: int = 3306) -> URL:
        """
        获取引擎需要的URL
        Args:
            db_name: 数据库名称
            username: 用户名
            password: 密码
            host: host
            port: port
        Returns:

        """
        return URL(drivername=self.dialect, username=username, password=password,
                   host=host, port=port, database=db_name)

    # noinspection DuplicatedCode
    def init_app(self, app):
        """
        mysql 实例初始化
        Args:
            app: app应用
        Returns:

        """
        self.app = app
        config: Dict = app.config if getattr(app, "config", None) else app.state.config

        self._verify_fastapi_app()  # 校验APP类型是否正确
        username = config.get("FESSQL_MYSQL_USERNAME") or self.username
        passwd = config.get("FESSQL_MYSQL_PASSWD") or self.passwd
        passwd = passwd if passwd is None else str(passwd)
        host = config.get("FESSQL_MYSQL_HOST") or self.host
        port = config.get("FESSQL_MYSQL_PORT") or self.port
        dbname = config.get("FESSQL_MYSQL_DBNAME") or self.dbname
        self.db_uri = self.get_engine_url(dbname, username=username, password=passwd, host=host, port=port)

        # 应用配置
        self.apply_engine_opts(config, self.engine_options)
        self.fessql_binds = config.get("FESSQL_BINDS") or self.fessql_binds
        self.verify_binds()

        # engine
        self.engine_pool[None] = self.create_engine(self.db_uri, self.engine_options)
        self.sessionmaker_pool[None] = self.create_scoped_sessionmaker(self.engine_pool[None])
        # 注册停止事件
        app.on_event('shutdown')(self.close_connection)

    # noinspection DuplicatedCode
    def init_engine(self, *, username: str = "root", passwd: str = None,
                    host: str = "127.0.0.1", port: int = 3306, dbname: str = None, **kwargs):
        """
        mysql 实例初始化
        Args:
            host:mysql host
            port:mysql port
            dbname: database name
            username: mysql user
            passwd: mysql password

        Returns:

        """
        username = username or self.username
        passwd = passwd or self.passwd
        passwd = passwd if passwd is None else str(passwd)
        host = host or self.host
        port = port or self.port
        dbname = dbname or self.dbname
        self.db_uri = self.get_engine_url(dbname, username=username, password=passwd, host=host, port=port)

        # 应用配置
        self.apply_engine_opts(kwargs, self.engine_options)
        self.fessql_binds = kwargs.pop("fessql_binds", None) or self.fessql_binds
        self.verify_binds()

        # engine
        self.engine_pool[None] = self.create_engine(self.db_uri, self.engine_options)
        self.sessionmaker_pool[None] = self.create_scoped_sessionmaker(self.engine_pool[None])
        # 注册停止事件
        atexit.register(self.close_connection)

    def close_connection(self, ):
        """
        释放连接
        Args:

        Returns:

        """
        for _, sessionmaker_ in self.sessionmaker_pool.items():
            sessionmaker_.remove()
        for _, engine_ in self.engine_pool.items():
            engine_.dispose()
        aelog.debug("清理所有数据库连接池完毕！")

    def _verify_fastapi_app(self, ):
        """
        校验APP类型是否正确

        暂时只支持fastapi框架
        Args:

        Returns:

        """

        try:
            from fastapi import FastAPI
        except ImportError as e:
            raise ImportError(f"FastAPI import error {e}.")
        else:
            if not isinstance(self.app, FastAPI):
                raise FuncArgsError("app type must be FastAPI.")

    def create_scoped_sessionmaker(self, bind: Engine) -> orm.scoped_session:
        """Create a :class:`~sqlalchemy.orm.scoping.scoped_session`
        on the factory from :meth:`create_session`.

        An extra key ``'scopefunc'`` can be set on the ``options`` dict to
        specify a custom scope function.  If it's not provided, Flask's app
        context stack identity is used. This will ensure that sessions are
        created and removed with the request/response cycle, and should be fine
        in most cases.

        :param bind: 引擎的bind
        """

        return orm.scoped_session(self.create_sessionmaker(bind=bind))

    def create_sessionmaker(self, bind: Engine) -> orm.sessionmaker:
        """Create the session factory used by :meth:`create_scoped_session`.

        The factory **must** return an object that SQLAlchemy recognizes as a session,
        or registering session events may raise an exception.

        Valid factories include a :class:`~sqlalchemy.orm.session.Session`
        class or a :class:`~sqlalchemy.orm.session.sessionmaker`.

        The default implementation creates a ``sessionmaker`` for

        :param bind: 引擎的bind
        """
        self.session_options.setdefault("query_cls", self.Query)
        return orm.sessionmaker(bind=bind, class_=FesSession, **self.session_options)

    @staticmethod
    def create_engine(sa_url: Union[str, URL], engine_opts: Dict[str, Any]) -> Engine:
        """
            Override this method to have final say over how the SQLAlchemy engine
            is created.

            In most cases, you will want to use ``'SQLALCHEMY_ENGINE_OPTIONS'``
            config variable or set ``engine_options`` for :func:`SQLAlchemy`.
        """
        return sqlalchemy.create_engine(sa_url, **engine_opts)

    def _create_engine(self, bind_key: str):
        """
        session bind
        Args:
            bind_key: engine pool one of connection
        Returns:

        """
        if bind_key not in self.fessql_binds:
            raise ValueError("bind is not exist, please config it in the FESSQL_BINDS.")
        if bind_key not in self.engine_pool:
            bind_conf: Dict = self.fessql_binds[bind_key]
            db_uri: URL = self.get_engine_url(bind_conf["fessql_mysql_dbname"],
                                              username=bind_conf["fessql_mysql_username"],
                                              password=bind_conf["fessql_mysql_passwd"],
                                              host=bind_conf["fessql_mysql_host"],
                                              port=bind_conf["fessql_mysql_port"])
            engine_options: Dict[str, Any] = {**self.engine_options}
            # 应用配置
            self.apply_engine_opts(bind_conf, engine_options)
            self.engine_pool[bind_key] = self.create_engine(db_uri, engine_options)

    def gen_sessionmaker(self, bind_key: str = None) -> orm.scoped_session:
        """
        session bind
        Args:
            bind_key: engine pool one of connection
        Returns:

        """
        if bind_key is not None and bind_key not in self.sessionmaker_pool:
            self._create_engine(bind_key)
            self.sessionmaker_pool[bind_key] = self.create_scoped_sessionmaker(self.engine_pool[bind_key])
        return self.sessionmaker_pool[bind_key]

    def ping_session(self, session: FesSession, reconnect=True) -> FesSession:
        """
        session探测,可以探测利用gen_session生成的session也可以探测默认的scope_session
        Args:
            session: session
            reconnect: 是否重连
        Returns:
            如果通则返回true,否则false
        """
        try:
            session.execute(text("SELECT 1")).first()
        except sqlalchemy_err.OperationalError as err:
            if reconnect:
                bind_key = getattr(session, "bind_key", "")
                if bind_key:
                    self.sessionmaker_pool[bind_key].remove()
                    session = self.gen_sessionmaker(bind_key)()
                    session.bind_key = bind_key  # 设置bind key
                else:
                    raise FuncArgsError(f"session中缺少bind_key变量") from err
            else:
                raise err
        # 返回重建后的session
        return session

    @contextmanager
    def gen_session(self, bind_key: str = None) -> Generator[FesSession, None, None]:
        """
        创建或者获取指定的session

        主要用于在一个视图内部针对同表不同库的数据请求获取
        Args:
            bind_key: bind中的bind key
        Returns:

        """

        session: FesSession = self.gen_sessionmaker(bind_key)()
        session.bind_key = bind_key  # 设置bind key
        session = self.ping_session(session)  # 校验重连,保证可用
        try:
            yield session
        finally:
            session.close()

    @contextmanager
    def insert_context(self, session: FesSession) -> Generator['FastapiAlchemy', None, None]:
        """
        插入数据context
        Args:
            session: session对象, 默认是self.session
        Returns:

        """
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
            raise HttpError(400, message=mysql_msg[1]["msg_zh"], error=e)

    @contextmanager
    def update_context(self, session: FesSession) -> Generator['FastapiAlchemy', None, None]:
        """
        更新数据context
        Args:
            session: session对象, 默认是self.session
        Returns:

        """
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
            raise HttpError(400, message=mysql_msg[2]["msg_zh"], error=e)

    @contextmanager
    def delete_context(self, session: FesSession) -> Generator['FastapiAlchemy', None, None]:
        """
        删除数据context
        Args:
            session: session对象, 默认是self.session
        Returns:

        """
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
            raise HttpError(400, message=mysql_msg[3]["msg_zh"], error=e)

    @staticmethod
    def _execute(session: FesSession, query: Union[FesQuery, str], params: Dict = None) -> ResultProxy:
        """
        插入数据，更新或者删除数据
        Args:
            query: SQL的查询字符串或者sqlalchemy表达式
            params: SQL表达式中的参数
            session: session对象, 默认是self.session
        Returns:
            不确定执行的是什么查询，直接返回ResultProxy实例
        """
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
            raise HttpError(400, message=mysql_msg[2]["msg_zh"], error=e)
        else:
            return cursor

    # noinspection DuplicatedCode
    def execute(self, session: FesSession, query: Union[FesQuery, str], params: Dict = None, size: int = None,
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
        cursor = self._execute(session, query, params)
        if size is None:
            resp = cursor.fetchall() if cursor.returns_rows else []
        elif size == 1:
            resp = cursor.fetchone() if cursor.returns_rows else None
        else:
            resp = cursor.fetchmany(size) if cursor.returns_rows else []

        if cursor_close is True:
            cursor.close()

        return resp
