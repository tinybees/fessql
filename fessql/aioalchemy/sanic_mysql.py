#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-25 下午4:58
"""
import asyncio
import atexit
from collections import MutableSequence
from math import ceil
from typing import (Any, Dict, List, MutableMapping, Optional, Tuple, Union)

import aelog
from aiomysql.sa import Engine, SAConnection, create_engine
from aiomysql.sa.exc import Error
from aiomysql.sa.result import ResultProxy, RowProxy
from pymysql.err import IntegrityError, MySQLError
from sqlalchemy.sql import Delete, Insert, Select, Update
from sqlalchemy.sql.elements import TextClause

from .query import Query
from .._alchemy import AlchemyMixIn
from .._err_msg import mysql_msg
from ..err import DBDuplicateKeyError, DBError, FuncArgsError, HttpError
from ..utils import _verify_message

__all__ = ("SanicMySQL", "Pagination", "Session")


# noinspection PyProtectedMember
class Pagination(object):
    """Internal helper class returned by :meth:`BaseQuery.paginate`.  You
    can also construct it from any other SQLAlchemy query object if you are
    working with other libraries.  Additionally it is possible to pass `None`
    as query object in which case the :meth:`prev` and :meth:`next` will
    no longer work.
    """

    def __init__(self, db_client: 'SessionReader', query: Query, total: int, items: List[RowProxy]):
        #: the unlimited query object that was used to create this
        #: aiomysqlclient object.
        self.session: SessionReader = db_client
        #: select query
        self._query: Query = query
        #: the current page number (1 indexed)
        self.page: int = query._page
        #: the number of items to be displayed on a page.
        self.per_page: int = query._per_page
        #: the total number of items matching the query
        self.total: int = total
        #: the items for the current page
        self.items: List[RowProxy] = items

    @property
    def pages(self) -> int:
        """The total number of pages"""
        if self.per_page == 0:
            pages = 0
        else:
            pages = int(ceil(self.total / float(self.per_page)))
        return pages

    async def prev(self, ) -> List[RowProxy]:
        """Returns a :class:`Pagination` object for the previous page."""
        self.page -= 1
        self._query._offset_clause = (self.page - 1) * self.per_page
        self._query.select_query()  # 重新生成分页SQL
        return await self.session._find_data(self._query)

    @property
    def prev_num(self) -> Optional[int]:
        """Number of the previous page."""
        if not self.has_prev:
            return None
        return self.page - 1

    @property
    def has_prev(self) -> bool:
        """True if a previous page exists"""
        return self.page > 1

    async def next(self, ) -> List[RowProxy]:
        """Returns a :class:`Pagination` object for the next page."""
        self.page += 1
        self._query._offset_clause = (self.page - 1) * self.per_page
        self._query.select_query()  # 重新生成分页SQL
        return await self.session._find_data(self._query)

    @property
    def has_next(self) -> bool:
        """True if a next page exists."""
        return self.page < self.pages

    @property
    def next_num(self) -> Optional[int]:
        """Number of the next page"""
        if not self.has_next:
            return None
        return self.page + 1


class BaseSession(object):
    """
    query session reader and writer
    """

    def __init__(self, aio_engine: Engine, message: Dict[int, Dict[str, Any]], msg_zh: str):
        """
            query session reader and writer
        Args:

        """
        self.aio_engine: Engine = aio_engine
        self.message: Dict[int, Dict[str, Any]] = message
        self.msg_zh: str = msg_zh


# noinspection PyProtectedMember
class SessionReader(BaseSession):
    """
    query session reader
    """

    async def _query_execute(self, query: Union[Select, str], params: Dict = None) -> ResultProxy:
        """
        查询数据

        # 读取的时候自动提交为true, 这样查询的时候就不用commit了
        # 因为如果是读写分离的操作,则发现写入commit后,再次读取的时候读取不到最新的数据,除非读取的时候手动增加commit的操作
        # 而这一步操作会感觉是非常不必要的,除非在同一个connection中才不用增加,而对于读写分离的操作是不现实的
        # 而读取的操作占多数设置自动commit后可以提高查询的效率,所以这里把此分开
        self.autocommit = True

        Args:
            query: SQL的查询字符串或者sqlalchemy表达式
            params: 执行的参数值,
        Returns:
            不确定执行的是什么查询，直接返回ResultProxy实例
        """
        conn: SAConnection = self.aio_engine.acquire()
        async with conn as conn:
            await conn.connection.autocommit(True)
            try:
                cursor = await conn.execute(query, params or {})
            except (MySQLError, Error) as e:
                aelog.exception("Find data failed, {}".format(e))
                raise HttpError(400, message=self.message[4][self.msg_zh])
            except Exception as e:
                aelog.exception(e)
                raise HttpError(400, message=self.message[4][self.msg_zh])

        return cursor

    async def _find_data(self, query: Query) -> List[RowProxy]:
        """
        查询单条数据
        Args:
            query: Query 查询类
        Returns:
            返回匹配的数据或者None
        """
        cursor = await self._query_execute(query._query_obj)
        return await cursor.fetchall() if cursor.returns_rows else []

    async def query_execute(self, query: Union[TextClause, str], params: Dict = None, size=None, cursor_close=True
                            ) -> Union[List[RowProxy], RowProxy, None]:
        """
        查询数据，用于复杂的查询
        Args:
            query: SQL的查询字符串
            size: 查询数据大小, 默认返回所有
            params: SQL表达式中的参数
            size: 查询数据大小, 默认返回所有
            cursor_close: 是否关闭游标，默认关闭，如果多次读取可以改为false，后面关闭的行为交给sqlalchemy处理

        Returns:
            List[RowProxy] or RowProxy or None
        """
        params = params if isinstance(params, MutableMapping) else {}
        cursor = await self._query_execute(query, params)

        if size is None:
            resp = await cursor.fetchall() if cursor.returns_rows else []
        elif size == 1:
            resp = await cursor.fetchone() if cursor.returns_rows else None
        else:
            resp = await cursor.fetchmany(size) if cursor.returns_rows else []

        if cursor_close is True:
            await cursor.close()

        return resp

    async def find_one(self, query: Query) -> Optional[RowProxy]:
        """
        查询单条数据
        Args:
            query: Query 查询类
        Returns:
            返回匹配的数据或者None
        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        cursor = await self._query_execute(query._query_obj)
        return await cursor.first() if cursor.returns_rows else None

    async def find_many(self, query: Query = None) -> Pagination:
        """
        查询多条数据,分页数据
        Args:
            query: Query 查询类
        Returns:
            Returns a :class:`Pagination` object.
        """

        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        items = await self._find_data(query)

        # No need to count if we're on the first page and there are fewer
        # items than we expected.
        if query._page == 1 and len(items) < query._per_page:
            total = len(items)
        else:
            total_result = await self.find_count(query)
            total = total_result.count

        return Pagination(self, query, total, items)

    async def find_all(self, query: Query) -> List[RowProxy]:
        """
        插入数据
        Args:
            query: Query 查询类
        Returns:

        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        return await self._find_data(query)

    async def find_count(self, query: Query) -> RowProxy:
        """
        查询数量
        Args:
            query: Query 查询类
        Returns:
            返回条数
        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        cursor = await self._query_execute(query._query_count_obj)
        return await cursor.first()


# noinspection PyProtectedMember
class SessionWriter(BaseSession):
    """
    query session writer
    """

    async def _execute(self, query: Union[Insert, Update, str], params: Union[List[Dict], Dict], msg_code: int
                       ) -> ResultProxy:
        """
        插入数据，更新或者删除数据
        Args:
            query: SQL的查询字符串或者sqlalchemy表达式
            params: 执行的参数值,可以是单个对象的字典也可以是多个对象的列表
            msg_code: 消息提示编码
        Returns:
            不确定执行的是什么查询，直接返回ResultProxy实例
        """
        conn: SAConnection = self.aio_engine.acquire()
        async with conn as conn:
            await conn.connection.autocommit(False)
            async with conn.begin() as trans:
                try:
                    cursor = await conn.execute(query, params)
                except IntegrityError as e:
                    await trans.rollback()
                    aelog.exception(e)
                    if "Duplicate" in str(e):
                        raise DBDuplicateKeyError(e)
                    else:
                        raise DBError(e)
                except (MySQLError, Error) as e:
                    await trans.rollback()
                    aelog.exception(e)
                    raise DBError(e)
                except Exception as e:
                    await trans.rollback()
                    aelog.exception(e)
                    raise HttpError(400, message=self.message[msg_code][self.msg_zh])

        return cursor

    async def _delete_execute(self, query: Union[Delete, str]) -> int:
        """
        删除数据
        Args:
            query: Query 查询类
        Returns:
            返回删除的条数
        """
        conn: SAConnection = self.aio_engine.acquire()
        async with conn as conn:
            await conn.connection.autocommit(False)
            async with conn.begin() as trans:
                try:
                    cursor = await conn.execute(query)
                except (MySQLError, Error) as e:
                    await trans.rollback()
                    aelog.exception(e)
                    raise DBError(e)
                except Exception as e:
                    await trans.rollback()
                    aelog.exception(e)
                    raise HttpError(400, message=self.message[3][self.msg_zh])

        return cursor.rowcount

    async def execute(self, query: Union[TextClause, str], params: Union[List[Dict], Dict]) -> int:
        """
        插入数据，更新或者删除数据
        Args:
            query: SQL的查询字符串
            params: 执行的参数值,可以是单个对象的字典也可以是多个对象的列表
        Returns:
            返回更新,插入或者删除影响的条数
        """
        params = params if isinstance(params, (MutableMapping, MutableSequence)) else {}
        cursor = await self._execute(query, params, 6)
        return cursor.rowcount

    async def insert_one(self, query: Query) -> Tuple[int, str]:
        """
        插入数据
        Args:
           query: Query 查询类
        Returns:
            (插入的条数,插入的ID)
        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")
        if not isinstance(query._insert_data, dict):
            raise FuncArgsError("query insert data type error!")

        cursor = await self._execute(query._query_obj, query._insert_data, 1)
        return cursor.rowcount, query._insert_data.get("id") or cursor.lastrowid

    async def insert_many(self, query: Query) -> int:
        """
        插入多条数据

        eg: User.insert().values([{"name": "test1"}, {"name": "test2"}]
        Args:
           query: Query 查询类
        Returns:
            插入的条数
        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")
        if not isinstance(query._insert_data, list):
            raise FuncArgsError("query insert data type error!")

        cursor = await self._execute(query._query_obj, query._insert_data, 1)
        return cursor.rowcount

    async def insert_from_select(self, query: Query) -> Tuple[int, str]:
        """
        查询并且插入数据, ``INSERT...FROM SELECT`` statement.

        e.g.::
            sel = select([table1.c.a, table1.c.b]).where(table1.c.c > 5)
            ins = table2.insert().from_select(['a', 'b'], sel)
        Args:
            query: Query 查询类
        Returns:
            (插入的条数,插入的ID)
        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        cursor = await self._execute(query._query_obj, {}, 1)
        return cursor.rowcount, cursor.lastrowid

    async def update_data(self, query: Query) -> int:
        """
        更新数据

        eg: where(User.c.id == bindparam("id")).values({"name": bindparam("name")})
         await conn.execute(sql, [{"id": 1, "name": "t1"}, {"id": 2, "name": "t2"}]
        Args:
            query: Query 查询类
        Returns:
            返回更新的条数
        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        cursor = await self._execute(query._query_obj, query._update_data, 2)
        return cursor.rowcount

    async def delete_data(self, query: Query) -> int:
        """
        删除数据
        Args:
            query: Query 查询类
        Returns:
            返回删除的条数
        """
        if not isinstance(query, Query):
            raise FuncArgsError("query type error!")

        return await self._delete_execute(query._query_obj)


class Session(SessionReader, SessionWriter):
    """
    query session reader and writer
    """
    pass


class SanicMySQL(AlchemyMixIn, object):
    """
    MySQL异步操作指南
    """

    def __init__(self, app=None, *, username: str = "root", passwd: str = None, host: str = "127.0.0.1",
                 port: int = 3306, dbname: str = None, pool_size: int = 10, **kwargs):
        """
        mysql 非阻塞工具类

        完整参数解释请参考aiomysql.Connection的文档
        Args:
            app: app应用
            username: mysql user
            passwd: mysql password
            host:mysql host
            port:mysql port
            dbname: database name
            pool_size: mysql pool size
            pool_recycle: pool recycle time, type int
            init_command: 初始执行的SQL
            connect_timeout: 连接超时时间
            autocommit: 是否自动commit,默认false
            fessql_binds: binds config, eg:{"first":{"fessql_mysql_host":"127.0.0.1",
                                                    "fessql_mysql_port":3306,
                                                    "fessql_mysql_username":"root",
                                                    "fessql_mysql_passwd":"",
                                                    "fessql_mysql_dbname":"dbname",
                                                    "fessql_mysql_pool_size":10}}

        """
        self.app = app
        self.engine_pool: Dict[Optional[str], Engine] = {}  # engine pool
        self.session_pool: Dict[Optional[str], Any] = {}  # session pool
        # default bind connection
        self.username = username
        self.passwd = passwd
        self.host = host
        self.port = port
        self.dbname = dbname
        self.pool_size = pool_size
        # other info
        self.pool_recycle = kwargs.pop("pool_recycle", 3600)  # free close time
        self.charset = "utf8mb4"
        self.fessql_binds: Dict = kwargs.pop("fessql_binds", {})  # binds config
        self.message = kwargs.pop("message", {})
        self.use_zh = kwargs.pop("use_zh", True)
        self.msg_zh: str = ""
        # self.autocommit = False  # 自动提交开关,默认和connection中的默认值一致
        self._conn_kwargs: Dict[str, Any] = kwargs  # 其他连接关键字参数

        if app is not None:
            self.init_app(app, username=self.username, passwd=self.passwd, host=self.host, port=self.port,
                          dbname=self.dbname, pool_size=self.pool_size, **self._conn_kwargs)

    def init_app(self, app, *, username: str = None, passwd: str = None, host: str = None, port: int = None,
                 dbname: str = None, pool_size: int = None, **kwargs):
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

        Returns:

        """
        self.app = app

        self._verify_sanic_app()  # 校验APP类型是否正确
        username = username or app.config.get("FESSQL_MYSQL_USERNAME", None) or self.username
        passwd = passwd or app.config.get("FESSQL_MYSQL_PASSWD", None) or self.passwd
        host = host or app.config.get("FESSQL_MYSQL_HOST", None) or self.host
        port = port or app.config.get("FESSQL_MYSQL_PORT", None) or self.port
        dbname = dbname or app.config.get("FESSQL_MYSQL_DBNAME", None) or self.dbname
        self.pool_size = pool_size or app.config.get("FESSQL_MYSQL_POOL_SIZE", None) or self.pool_size

        self.pool_recycle = kwargs.pop("pool_recycle", None) or app.config.get(
            "FESSQL_POOL_RECYCLE", None) or self.pool_recycle

        message = kwargs.pop("message", None) or app.config.get("FESSQL_MYSQL_MESSAGE", None) or self.message
        use_zh = kwargs.pop("use_zh", None) or app.config.get("FESSQL_MYSQL_MSGZH", None) or self.use_zh

        self.fessql_binds = kwargs.pop("fessql_binds", None) or app.config.get(
            "FESSQL_BINDS", None) or self.fessql_binds
        self.verify_binds()

        passwd = passwd if passwd is None else str(passwd)
        self.message = _verify_message(mysql_msg, message)
        self.msg_zh = "msg_zh" if use_zh else "msg_en"
        self._conn_kwargs = kwargs

        @app.listener('before_server_start')
        async def open_connection(app_, loop):
            """

            Args:

            Returns:

            """
            # engine
            self.engine_pool[None] = await create_engine(
                host=host, port=port, user=username, password=passwd, db=dbname, maxsize=self.pool_size,
                pool_recycle=self.pool_recycle, charset=self.charset, **self._conn_kwargs)

        @app.listener('after_server_stop')
        async def close_connection(app_, loop):
            """

            Args:

            Returns:

            """
            tasks = []
            for _, aio_engine in self.engine_pool.items():
                aio_engine.close()
                tasks.append(asyncio.ensure_future(aio_engine.wait_closed()))
            await asyncio.wait(tasks)
            aelog.debug("清理所有数据库连接池完毕！")

    def init_engine(self, *, username: str = "root", passwd: str = None, host: str = "127.0.0.1", port: int = 3306,
                    dbname: str = None, pool_size: int = 50, **kwargs):
        """
        mysql 实例初始化
        Args:
            host:mysql host
            port:mysql port
            dbname: database name
            username: mysql user
            passwd: mysql password
            pool_size: mysql pool size

        Returns:

        """
        username = username or self.username
        passwd = passwd or self.passwd
        host = host or self.host
        port = port or self.port
        dbname = dbname or self.dbname
        self.pool_size = pool_size or self.pool_size

        self.pool_recycle = kwargs.pop("pool_recycle", None) or self.pool_recycle

        message = kwargs.pop("message", None) or self.message
        use_zh = kwargs.pop("use_zh", None) or self.use_zh

        self.fessql_binds = kwargs.pop("fessql_binds", None) or self.fessql_binds
        self.verify_binds()

        passwd = passwd if passwd is None else str(passwd)
        self.message = _verify_message(mysql_msg, message)
        self.msg_zh = "msg_zh" if use_zh else "msg_en"
        loop = asyncio.get_event_loop()
        self._conn_kwargs = kwargs

        async def open_connection():
            """

            Args:

            Returns:

            """
            # engine
            self.engine_pool[None] = await create_engine(
                host=host, port=port, user=username, password=passwd, db=dbname, maxsize=self.pool_size,
                pool_recycle=self.pool_recycle, charset=self.charset, **self._conn_kwargs)

        async def close_connection():
            """

            Args:

            Returns:

            """
            tasks = []
            for _, aio_engine in self.engine_pool.items():
                aio_engine.close()
                tasks.append(asyncio.ensure_future(aio_engine.wait_closed(), loop=loop))
            await asyncio.wait(tasks)

        loop.run_until_complete(open_connection())
        atexit.register(lambda: loop.run_until_complete(close_connection()))

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

    @property
    def query(self, ) -> Query:
        """

        Args:

        Returns:

        """
        return Query()

    async def _create_engine(self, bind: str):
        """
        session bind
        Args:
            bind: engine pool one of connection
        Returns:

        """
        if bind not in self.fessql_binds:
            raise ValueError("bind is not exist, please config it in the FESSQL_BINDS.")
        if bind not in self.engine_pool:
            bind_conf: Dict = self.fessql_binds[bind]
            self.engine_pool[bind] = await create_engine(
                host=bind_conf.get("fessql_mysql_host"), port=bind_conf.get("fessql_mysql_port"),
                user=bind_conf.get("fessql_mysql_username"), password=bind_conf.get("fessql_mysql_passwd"),
                db=bind_conf.get("fessql_mysql_dbname"),
                maxsize=bind_conf.get("fessql_mysql_pool_size") or self.pool_size,
                pool_recycle=self.pool_recycle, charset=self.charset, **self._conn_kwargs)

    @property
    def session(self, ) -> Session:
        """
        session default bind
        Args:

        Returns:

        """
        if None not in self.engine_pool:
            raise ValueError("Default bind is not exist.")
        if None not in self.session_pool:
            self.session_pool[None] = Session(self.engine_pool[None], self.message, self.msg_zh)
        return self.session_pool[None]

    async def gen_session(self, bind: str) -> Session:
        """
        session bind
        Args:
            bind: engine pool one of connection
        Returns:

        """
        await self._create_engine(bind)
        if bind not in self.session_pool:
            self.session_pool[bind] = Session(self.engine_pool[bind], self.message, self.msg_zh)
        return self.session_pool[bind]
