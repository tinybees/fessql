#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/11/10 下午4:14
"""
from typing import Any, Dict, Optional

from .dbalchemy import DBAlchemy
from .drivers import DialectDriver
from ..err import FuncArgsError

__all__ = ("FastapiAlchemy", "FlaskAlchemy")


class FastapiAlchemy(DBAlchemy):
    """
    DB同步操作指南，适用于fastapi
    """

    def __init__(self, app=None, *, username: str = "root", passwd: str = "", host: str = "127.0.0.1",
                 port: int = 3306, dbname: str = "", dialect: str = DialectDriver.mysql_pymysql,
                 fessql_binds: Optional[Dict[str, Dict]] = None, session_options: Optional[Dict[str, Any]] = None,
                 engine_options: Optional[Dict[str, Any]] = None, **kwargs):
        """
        DB同步操作指南，适用于fastapi,基于SQlalchemy
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
        super().__init__(app, username=username, passwd=passwd, host=host, port=port, dbname=dbname,
                         dialect=dialect, fessql_binds=fessql_binds, session_options=session_options,
                         engine_options=engine_options, **kwargs)

    def init_app(self, app) -> None:
        """
        mysql 实例初始化
        Args:
            app: app应用
        Returns:

        """
        self.app = app
        self._verify_fastapi_app()  # 校验APP类型是否正确

        config: Dict[str, Any] = app.config if getattr(app, "config", None) else app.state.config
        self._init_app(config)
        # 注册停止事件
        app.on_event('shutdown')(self.close_connection)

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


class FlaskAlchemy(DBAlchemy):
    """
    DB同步操作指南，适用于Flask
    """

    def __init__(self, app=None, *, username: str = "root", passwd: str = "", host: str = "127.0.0.1",
                 port: int = 3306, dbname: str = "", dialect: str = DialectDriver.mysql_pymysql,
                 fessql_binds: Optional[Dict[str, Dict]] = None, session_options: Optional[Dict[str, Any]] = None,
                 engine_options: Optional[Dict[str, Any]] = None, **kwargs):
        """
        DB同步操作指南，适用于Flask,基于SQlalchemy
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
        super().__init__(app, username=username, passwd=passwd, host=host, port=port, dbname=dbname,
                         dialect=dialect, fessql_binds=fessql_binds, session_options=session_options,
                         engine_options=engine_options, **kwargs)

    def init_app(self, app) -> None:
        """
        mysql 实例初始化
        Args:
            app: app应用
        Returns:

        """
        self.app = app
        self._verify_flask_app()  # 校验APP类型是否正确

        config: Dict[str, Any] = app.config
        self._init_app(config)

        # 注册停止事件
        @app.teardown_appcontext
        def _shutdown_close_connection(response_or_exc):
            self.close_connection()
            return response_or_exc

    def _verify_flask_app(self, ):
        """
        校验APP类型是否正确

        暂时只支持fastapi框架
        Args:

        Returns:

        """

        try:
            from flask import Flask
        except ImportError as e:
            raise ImportError(f"Flask import error {e}.")
        else:
            if not isinstance(self.app, Flask):
                raise FuncArgsError("app type must be Flask.")
