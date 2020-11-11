#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/11/11 下午5:41
"""
__all__ = ("DialectDriver",)


class DialectDriver(object):
    """
    数据库方言驱动
    """
    #  postgresql
    pg_default = "postgresql+psycopg2"  # default
    pg_pg8000 = "postgresql+pg8000"
    # mysql
    mysql_default = "mysql+mysqldb"  # default
    mysql_pymysql = "mysql+pymysql"
    # oracle
    oracle_cx = "oracle+cx_oracle"  # default
    # SQL Server
    mssql_default = "mssql+pyodbc"  # default
    mssql_pymssql = "mssql+pymssql"
    # SQLite
    sqlite = "sqlite:///"
