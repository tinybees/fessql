#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/11/10 下午4:14
"""
from typing import Any, Dict

from .dbalchemy import DBAlchemy
from ..err import FuncArgsError

__all__ = ("FastapiAlchemy", "FlaskAlchemy")


class FastapiAlchemy(DBAlchemy):
    """
    DB同步操作指南，适用于fastapi
    """

    def init_app(self, app):
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
    Flask同步操作指南
    """

    def init_app(self, app):
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
