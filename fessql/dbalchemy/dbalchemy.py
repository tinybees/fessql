#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2024/6/11 下午6:52


"""
import atexit
from collections import MutableMapping
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional, Type, Union

import aelog
import sqlalchemy
from sqlalchemy import exc as sqlalchemy_err, orm, text
# noinspection PyProtectedMember
from sqlalchemy.engine import Engine
from sqlalchemy.engine.result import ResultProxy, RowProxy
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import DatabaseError, IntegrityError

from fessql._alchemy import AlchemyMixIn
from fessql._err_msg import mysql_msg
from fessql.err import DBDuplicateKeyError, DBError, FuncArgsError, HttpError
from ._query import FesQuery
from .drivers import DialectDriver

__all__ = ("FesSession", "FesMgrSession", "DBAlchemy")


class FesSession(orm.Session):
    """
    改造orm的session类使得能够自动提示query的所有方法
    """

    def __init__(self, autocommit: bool = False, autoflush: bool = True, expire_on_commit=False,
                 query_cls: Type[FesQuery] = FesQuery, **options):
        """
            改造orm的session类使得能够自动提示query的所有方法
        Args:

        """
        super().__init__(autocommit=autocommit, autoflush=autoflush, expire_on_commit=expire_on_commit,
                         query_cls=query_cls, **options)
        self.bind_key: Optional[str] = None
        self.is_closed: bool = False  # session是否关闭
        # noinspection PyTypeChecker
        self.mgr_session: Optional['FesMgrSession'] = None

    # noinspection PyTypeChecker
    def query(self, *entities, **kwargs) -> FesQuery:
        """Return a new :class:`.FesQuery` object corresponding to this
        :class:`.Session`.
        返回新FesQuery的对象实例
        """
        kwargs.setdefault("mgr_session", self.mgr_session)
        return super().query(*entities, **kwargs)

    def close(self):
        """Close this Session.

        This clears all items and ends any transaction in progress.

        If this session were created with ``autocommit=False``, a new
        transaction is immediately begun.  Note that this new transaction does
        not use any connection resources until they are first needed.

        """
        super().close()
        self.is_closed = True


class FesMgrSession(object):
    """
    单个session的工厂管理类
    """

    def __init__(self, scoped_session: orm.scoped_session, bind_key: Optional[str] = None):
        """
        单个session的工厂管理类
        Args:

        """
        self._scoped_session: orm.scoped_session = scoped_session
        self.bind_key: Optional[str] = bind_key

    def sessfes(self, ) -> FesSession:
        """
        返回FesSession对象实例

        主要用于多个FesQuery需要同一个session的情况，比如union查询
        Args:
        Returns:

        """
        sessfes: FesSession = self._scoped_session()
        sessfes.bind_key = self.bind_key
        sessfes.mgr_session = self
        return sessfes

    def query(self, *entities, **kwargs) -> FesQuery:
        """Return a new :class:`.FesQuery` object corresponding to this
        :class:`.Session`.
        返回包含新FesSession对象实例的新FesQuery的对象实例
        """
        kwargs.setdefault("mgr_session", self)
        return self.sessfes().query(*entities, **kwargs)

    def execute(self, query: Union[FesQuery, str], params: Optional[Dict[str, Any]] = None) -> Optional[RowProxy]:
        """
        插入数据，更新或者删除数据
        Args:
            query: SQL的查询字符串或者sqlalchemy表达式
            params: SQL表达式中的参数
        Returns:
            不确定执行的是什么查询，直接返回RowProxy实例
        """
        session: FesSession = self.sessfes()
        cursor: Optional[ResultProxy] = None
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
            return cursor.fetchone() if cursor.returns_rows else None
        finally:
            if cursor:
                cursor.close()
            session.close()

    def query_execute(self, query: Union[FesQuery, str], params: Optional[Dict[str, Any]] = None,
                      size: Optional[int] = None) -> Union[List[RowProxy], RowProxy, None]:
        """
        查询数据
        Args:
            query: SQL的查询字符串或者sqlalchemy表达式
            params: SQL表达式中的参数
            size: 查询数据大小, 默认返回所有
            # cursor_close: 是否关闭游标，默认关闭，如果多次读取可以改为false，后面关闭的行为交给sqlalchemy处理
        Returns:
            List[RowProxy] or RowProxy or None
        """
        params = dict(params) if isinstance(params, MutableMapping) else {}

        session: FesSession = self.sessfes()
        cursor: Optional[ResultProxy] = None
        try:
            cursor = session.execute(query, params)
            if size is None:
                resp = cursor.fetchall() if cursor.returns_rows else []
            elif size == 1:
                resp = cursor.fetchone() if cursor.returns_rows else None
            else:
                resp = cursor.fetchmany(size) if cursor.returns_rows else []
        finally:
            if cursor:
                cursor.close()
            session.close()

        return resp


class DBAlchemy(AlchemyMixIn, object):
    """
    DB同步操作指南,基于SQlalchemy
    """

    def __init__(self, app=None, *, username: str = "root", passwd: str = "", host: str = "127.0.0.1",
                 port: int = 3306, dbname: str = "", dialect: str = DialectDriver.mysql_pymysql,
                 fessql_binds: Optional[Dict[str, Dict]] = None, session_options: Optional[Dict[str, Any]] = None,
                 engine_options: Optional[Dict[str, Any]] = None, **kwargs):
        """
        DB同步操作指南,基于SQlalchemy
        Args:
            app: app应用
            username: mysql user
            passwd: mysql password
            host:mysql host
            port:mysql port
            dbname: database name
            dialect: sqlalchemy默认的Dialect驱动
            fessql_binds: fesql binds
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
        self.dbname: str = dbname
        self.db_uri: URL = URL("")
        # session and engine
        self.kwargs: Dict[str, Any] = kwargs
        self.session_options: Dict[str, Any] = session_options or {}
        self._set_session_opts()  # 更新默认参数
        self.engine_options: Dict[str, Any] = engine_options or {}
        self._set_engine_opts()  # 更新默认参数
        # other binds
        self.fessql_binds: Dict[str, Dict] = fessql_binds or {}  # binds config

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
        self.session_options.setdefault("expire_on_commit", self.kwargs.get("expire_on_commit", False))

    def _set_engine_opts(self, ):
        """
        设置创建引擎的默认值
        Args:

        Returns:

        """
        self.engine_options.setdefault("pool_size", self.kwargs.get("pool_size", 25))
        self.engine_options.setdefault("pool_recycle", self.kwargs.get("pool_recycle", 3600))
        self.engine_options.setdefault("pool_timeout", self.kwargs.get("pool_timeout", 60))
        self.engine_options.setdefault("max_overflow", self.kwargs.get("max_overflow", 10))
        self.engine_options.setdefault("pool_use_lifo", self.kwargs.get("pool_use_lifo", True))
        self.engine_options.setdefault("echo", self.kwargs.get("echo", False))
        self.engine_options.setdefault("connect_args", {
            "connect_timeout": self.kwargs.get("connect_timeout", 60),
            "charset": self.charset, "binary_prefix": self.binary_prefix})

    @staticmethod
    def _apply_engine_opts(configs: Dict[str, Any], options: Dict[str, Any]):
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

    def get_engine_url(self, db_name, *, username: str = "", password: Optional[str] = None, host="127.0.0.1",
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
    def init_app(self, app) -> None:
        """
        mysql 实例初始化
        Args:
            app: app应用
        Returns:

        """
        raise NotImplementedError

    # noinspection DuplicatedCode
    def _init_app(self, config: Dict[str, Any]):
        """
        初始化APP
        Args:
            config: APP的配置信息
        Returns:

        """
        username = config.get("FESSQL_MYSQL_USERNAME", "") or self.username
        passwd: Optional[str] = config.get("FESSQL_MYSQL_PASSWD", "") or self.passwd
        passwd = passwd if passwd is None else str(passwd)
        host = config.get("FESSQL_MYSQL_HOST", "") or self.host
        port = config.get("FESSQL_MYSQL_PORT", "") or self.port
        dbname = config.get("FESSQL_MYSQL_DBNAME", "") or self.dbname
        self.db_uri = self.get_engine_url(dbname, username=username, password=passwd, host=host, port=port)

        # 应用配置
        self._apply_engine_opts(config, self.engine_options)
        self.fessql_binds = config.get("FESSQL_BINDS") or self.fessql_binds
        self.verify_binds()

        # engine
        self.engine_pool[None] = self._create_engine(self.db_uri, self.engine_options)
        self.sessionmaker_pool[None] = self._create_scoped_sessionmaker(self.engine_pool[None])

    # noinspection DuplicatedCode
    def init_engine(self, *, username: str = "root", passwd: Optional[str] = "",
                    host: str = "127.0.0.1", port: int = 3306, dbname: str = "", **kwargs) -> None:
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
        self._apply_engine_opts(kwargs, self.engine_options)
        self.fessql_binds = kwargs.pop("fessql_binds", None) or self.fessql_binds
        self.verify_binds()

        # engine
        self.engine_pool[None] = self._create_engine(self.db_uri, self.engine_options)
        self.sessionmaker_pool[None] = self._create_scoped_sessionmaker(self.engine_pool[None])
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

    def _create_scoped_sessionmaker(self, bind: Engine) -> orm.scoped_session:
        """Create a :class:`~sqlalchemy.orm.scoping.scoped_session`
        on the factory from :meth:`create_session`.

        An extra key ``'scopefunc'`` can be set on the ``options`` dict to
        specify a custom scope function.  If it's not provided, Flask's app
        context stack identity is used. This will ensure that sessions are
        created and removed with the request/response cycle, and should be fine
        in most cases.

        :param bind: 引擎的bind
        """

        return orm.scoped_session(self._create_sessionmaker(bind=bind))

    def _create_sessionmaker(self, bind: Engine) -> orm.sessionmaker:
        """Create the session factory used by :meth:`create_scoped_session`.

        The factory **must** return an object that SQLAlchemy recognizes as a session,
        or registering session events may raise an exception.

        Valid factories include a :class:`~sqlalchemy.orm.session.Session`
        class or a :class:`~sqlalchemy.orm.session.sessionmaker`.

        The default implementation creates a ``sessionmaker`` for

        :param bind: 引擎的bind
        """
        # query_class: 查询类,orm.Query的子类
        self.session_options.setdefault("query_cls", FesQuery)
        return orm.sessionmaker(bind=bind, class_=FesSession, **self.session_options)

    @staticmethod
    def _create_engine(sa_url: Union[str, URL], engine_opts: Dict[str, Any]) -> Engine:
        """
            Override this method to have final say over how the SQLAlchemy engine
            is created.

            In most cases, you will want to use ``'SQLALCHEMY_ENGINE_OPTIONS'``
            config variable or set ``engine_options`` for :func:`SQLAlchemy`.
        """
        return sqlalchemy.create_engine(sa_url, **engine_opts)

    def _create_pool_engine(self, bind_key: str):
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
            self._apply_engine_opts(bind_conf, engine_options)
            self.engine_pool[bind_key] = self._create_engine(db_uri, engine_options)

    def _gen_sessionmaker(self, bind_key: Optional[str] = None) -> orm.scoped_session:
        """
        session bind
        Args:
            bind_key: engine pool one of connection
        Returns:

        """
        if bind_key is not None and bind_key not in self.sessionmaker_pool:
            self._create_pool_engine(bind_key)
            self.sessionmaker_pool[bind_key] = self._create_scoped_sessionmaker(self.engine_pool[bind_key])
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
                if bind_key != "":
                    self.sessionmaker_pool[bind_key].remove()
                    session = self.gen_session(bind_key).sessfes()
                else:
                    raise FuncArgsError(f"session中缺少bind_key变量") from err
            else:
                raise err
        # 返回重建后的session
        return session

    def gen_session(self, bind_key: Optional[str] = None) -> FesMgrSession:
        """
        创建或者获取指定的session

        主要用于在一个视图内部针对同表不同库的数据请求获取
        Args:
            bind_key: bind中的bind key
        Returns:

        """

        return FesMgrSession(self._gen_sessionmaker(bind_key), bind_key)

    @property
    def session(self, ) -> FesMgrSession:
        """
        默认的session

        主要用于在一个视图内部针对同表不同库的数据请求获取
        Args:

        Returns:

        """

        return self.gen_session()

    @staticmethod
    @contextmanager
    def insert_context(session: FesMgrSession) -> Generator[FesSession, None, None]:
        """
        插入数据context
        Args:
            session: FesMgrSession对象
        Returns:

        """
        sessfes: FesSession = session.sessfes()
        try:
            yield sessfes
            sessfes.commit()
        except IntegrityError as e:
            sessfes.rollback()
            if "Duplicate" in str(e):
                raise DBDuplicateKeyError(e)
            else:
                raise DBError(e)
        except DatabaseError as e:
            sessfes.rollback()
            aelog.exception(e)
            raise DBError(e)
        except Exception as e:
            sessfes.rollback()
            aelog.exception(e)
            raise HttpError(400, message=mysql_msg[1]["msg_zh"], error=e)
        finally:
            sessfes.close()

    @staticmethod
    @contextmanager
    def update_context(session: FesMgrSession) -> Generator[FesSession, None, None]:
        """
        更新数据context
        Args:
            session: FesMgrSession对象
        Returns:

        """
        sessfes: FesSession = session.sessfes()
        try:
            yield sessfes
            sessfes.commit()
        except IntegrityError as e:
            sessfes.rollback()
            if "Duplicate" in str(e):
                raise DBDuplicateKeyError(e)
            else:
                raise DBError(e)
        except DatabaseError as e:
            sessfes.rollback()
            aelog.exception(e)
            raise DBError(e)
        except Exception as e:
            sessfes.rollback()
            aelog.exception(e)
            raise HttpError(400, message=mysql_msg[2]["msg_zh"], error=e)
        finally:
            sessfes.close()

    @staticmethod
    @contextmanager
    def delete_context(session: FesMgrSession) -> Generator[FesSession, None, None]:
        """
        删除数据context
        Args:
            session: FesMgrSession对象
        Returns:

        """
        sessfes: FesSession = session.sessfes()
        try:
            yield sessfes
            sessfes.commit()
        except DatabaseError as e:
            sessfes.rollback()
            aelog.exception(e)
            raise DBError(e)
        except Exception as e:
            sessfes.rollback()
            aelog.exception(e)
            raise HttpError(400, message=mysql_msg[3]["msg_zh"], error=e)
        finally:
            sessfes.close()
