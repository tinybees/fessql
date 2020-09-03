#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 19-4-2 上午9:04
"""
from typing import Dict, List, Optional, Tuple

import aelog
import pymysql
from pymysql.connections import Connection

__all__ = ("TinyMysql",)


class TinyMysql(object):
    """
        pymysql 操作数据库的各种方法
    Args:

    Returns:

    """

    def __init__(self, db_user: str, db_pwd: str, db_host: str = "127.0.0.1", db_port: int = 3306,
                 db_name: str = None):
        """
            pymysql 操作数据库的各种方法
        Args:
            db_user: 用户名
            db_pwd: 密码
            db_host: host
            db_port: port
            db_name: 数据库名称
        Returns:

        """
        self._conn: Optional[Connection] = None
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pwd = db_pwd
        self.db_name = db_name

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        del exc_info
        self.close()

    @property
    def conn(self, ) -> Connection:
        """
        获取MySQL的连接对象
        """

        def _get_connection() -> Connection:
            return pymysql.connect(host=self.db_host, port=self.db_port, db=self.db_name, user=self.db_user,
                                   passwd=self.db_pwd, charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor)

        if self._conn is None:
            self._conn = _get_connection()
        else:
            try:
                self._conn.ping()
            except pymysql.Error:
                self._conn = _get_connection()

        return self._conn

    def close(self, ):
        """
        关闭连接
        Args:

        Returns:

        """
        self.conn.close()

    def execute_many(self, sql: str, args_data: List[Tuple]) -> int:
        """
            批量插入数据
        Args:
            sql: 插入的SQL语句
            args_data: 批量插入的数据，为一个包含元祖的列表

            If args is a list or tuple, %s can be used as a placeholder in the query.
            If args is a dict, %(name)s can be used as a placeholder in the query.

        Returns:
        INSERT INTO traffic_100 (IMEI,lbs_dict_id,app_key) VALUES(%s,%s,%s)
        [('868403022323171', None, 'EB23B21E6E1D930E850E7267E3F00095'),
        ('865072026982119', None, 'EB23B21E6E1D930E850E7267E3F00095')]

        """

        count: int = 0
        try:
            with self.conn.cursor() as cursor:
                count = cursor.executemany(sql, args_data)  # type: ignore
        except pymysql.Error as e:
            self.conn.rollback()
            aelog.exception(e)
        except Exception as e:
            self.conn.rollback()
            aelog.exception(e)
        else:
            self.conn.commit()
        return count

    def execute(self, sql: str, args_data: Tuple = None) -> int:
        """
            执行单条记录，更新、插入或者删除
        Args:
            sql: 插入的SQL语句
            args_data: tuple, list or dict, 批量插入的数据，为一个包含元祖的列表

            If args is a list or tuple, %s can be used as a placeholder in the query.
            If args is a dict, %(name)s can be used as a placeholder in the query.
        Returns:
        INSERT INTO traffic_100 (IMEI,lbs_dict_id,app_key) VALUES(%s,%s,%s)
        ('868403022323171', None, 'EB23B21E6E1D930E850E7267E3F00095')

        """

        count = 0
        try:
            with self.conn.cursor() as cursor:
                count = cursor.execute(sql, args_data)
        except pymysql.Error as e:
            self.conn.rollback()
            aelog.exception(e)
        except Exception as e:
            self.conn.rollback()
            aelog.exception(e)
        else:
            self.conn.commit()
        return count

    def find_one(self, sql: str, args: Tuple = None) -> Optional[Dict]:
        """
            查询单条记录
        Args:
            sql: sql 语句
            args: 查询参数
        Returns:
            返回单条记录的返回值
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, args)
        except pymysql.Error as e:
            aelog.exception(e)
            return None
        else:
            return cursor.fetchone()  # type: ignore

    def find_data(self, sql: str, args: Tuple = None, size: int = None) -> List[Dict]:
        """
            查询指定行数的数据
        Args:
            sql: sql 语句
            args: 查询参数
            size: 返回记录的条数
        Returns:
            返回包含指定行数数据的列表,或者所有行数数据的列表
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, args)
        except pymysql.Error as e:
            aelog.exception(e)
            return []
        else:
            return cursor.fetchall() if not size else cursor.fetchmany(size)  # type: ignore
