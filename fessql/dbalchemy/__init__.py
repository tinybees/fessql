#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/9/3 上午11:30
"""

from .flask_alchemy import *
from .fastapi_alchemy import *


__all__ = (
    "FlaskAlchemy", "DialectDriver",
    "FastapiPagination", "FastapiQuery", "FastapiAlchemy",
)


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
