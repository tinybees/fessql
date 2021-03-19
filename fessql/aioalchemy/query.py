#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/3/1 上午12:00
"""

from typing import (Dict, List, MutableMapping, Optional, Union)

import aelog
from aiomysql.sa import exc
# noinspection PyProtectedMember
from aiomysql.sa.connection import _distill_params, noop
# noinspection PyProtectedMember
from aiomysql.sa.engine import _dialect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql import (Delete, Insert, Select, Update, delete, func, insert, select, update)
from sqlalchemy.sql.dml import UpdateBase

from ..err import FuncArgsError, QueryArgsError

__all__ = ("Query",)


class BaseQuery(object):
    """
    查询
    """

    def __init__(self, ):
        """
            查询
        Args:

        """
        self._model: Optional[DeclarativeMeta] = None
        self._whereclause: List = []
        self._order_by: List = []
        self._group_by: List = []
        self._having: List = []
        self._distinct: List = []
        self._columns: List = []
        self._union: List = []
        self._union_all: List = []
        self._with_hint: List = []
        self._bind_values: List = []
        # limit, offset
        self._limit_clause: Optional[int] = None
        self._offset_clause: Optional[int] = None

    def where(self, *whereclause) -> 'BaseQuery':
        """return basequery construct with the given expression added to
        its WHERE clause, joined to the existing clause via AND, if any.

        """

        self._whereclause.extend(whereclause)
        return self

    def model(self, modelclause: DeclarativeMeta) -> 'BaseQuery':
        """
        return basequery construct with the given expression added to
        its model clause.

        Arg:
            modelclause: sqlalchemy中的model或者table
        """
        if not isinstance(modelclause, DeclarativeMeta):
            raise FuncArgsError("model type error!")

        self._model = modelclause
        return self

    table = model

    def order_by(self, *clauses) -> 'BaseQuery':
        """return basequery with the given list of ORDER BY
        criterion applied.

        The criterion will be appended to any pre-existing ORDER BY
        criterion.

        """

        self._order_by.extend(clauses)
        return self

    def group_by(self, *clauses) -> 'BaseQuery':
        """return basequery with the given list of GROUP BY
        criterion applied.

        The criterion will be appended to any pre-existing GROUP BY
        criterion.

        """

        self._group_by.extend(clauses)
        return self

    def having(self, *having) -> 'BaseQuery':
        """return basequery construct with the given expression added to
        its HAVING clause, joined to the existing clause via AND, if any.

        """
        self._having.extend(having)
        return self

    def distinct(self, *expr) -> 'BaseQuery':
        r"""Return basequery construct which will apply DISTINCT to its
        columns clause.

        :param expr: optional column expressions.  When present,
         the PostgreSQL dialect will render a ``DISTINCT ON (<expressions>>)``
         construct.

        """
        self._distinct.extend(expr)
        return self

    def columns(self, *columns) -> 'BaseQuery':
        r"""Return basequery :func:`.select` construct with its columns
        clause replaced with the given columns.

        This method is exactly equivalent to as if the original
        :func:`.select` had been called with the given columns
        clause.   I.e. a statement::

            s = select([table1.c.a, table1.c.b])
            s = s.with_only_columns([table1.c.b])

        This means that FROM clauses which are only derived
        from the column list will be discarded if the new column
        list no longer contains that FROM::

        """
        self._columns.extend(columns)
        return self

    def union(self, other, **kwargs) -> 'BaseQuery':
        """return a SQL UNION of this select() construct against the given
        selectable."""

        self._union = [other, kwargs]
        return self

    def union_all(self, other, **kwargs) -> 'BaseQuery':
        """return a SQL UNION ALL of this select() construct against the given
        selectable.

        """
        self._union_all = [other, kwargs]
        return self

    def with_hint(self, selectable, text_, dialect_name='*') -> 'BaseQuery':
        r"""Add an indexing or other executional context hint for the given
        selectable to this :class:`.Select`.

        The text of the hint is rendered in the appropriate
        location for the database backend in use, relative
        to the given :class:`.Table` or :class:`.Alias` passed as the
        ``selectable`` argument. The dialect implementation
        typically uses Python string substitution syntax
        with the token ``%(name)s`` to render the name of
        the table or alias. E.g. when using Oracle, the
        following::

            select([mytable]).\
                with_hint(mytable, "index(%(name)s ix_mytable)")

        Would render SQL as::

            select /*+ index(mytable ix_mytable) */ ... from mytable

        The ``dialect_name`` option will limit the rendering of a particular
        hint to a particular backend. Such as, to add hints for both Oracle
        and Sybase simultaneously::

            select([mytable]).\
                with_hint(mytable, "index(%(name)s ix_mytable)", 'oracle').\
                with_hint(mytable, "WITH INDEX ix_mytable", 'sybase')

        .. seealso::

            :meth:`.Select.with_statement_hint`

        """
        self._with_hint = [selectable, text_, dialect_name]
        return self

    def values(self, *args) -> 'BaseQuery':
        r"""specify a fixed VALUES clause for an SET clause for an UPDATE."""
        self._bind_values.extend(args)
        return self


# noinspection PyProtectedMember
class Query(BaseQuery):
    """
    查询
    """

    def __init__(self, ):
        """

        Args:

        Returns:

        """
        # data
        self._insert_data: Union[List[Dict], Dict] = {}
        self._update_data: Union[List[Dict], Dict] = {}
        # query
        self._query_obj: Optional[Union[Select, Insert, Update, Delete]] = None
        self._query_count_obj: Optional[Select] = None  # 查询数量select
        #: the current page number (1 indexed)
        self._page: int = 1
        #: the number of items to be displayed on a page.
        self._per_page: int = 20

        super().__init__()

    def _get_model_default_value(self, ) -> Dict:
        """
        获取insert默认值
        Args:
        Returns:

        """
        default_values = {}
        for key, val in self._model.__dict__.items():
            if not key.startswith("_") and isinstance(val, InstrumentedAttribute):
                if val.default:
                    if val.default.is_callable:
                        default_values[key] = val.default.arg.__wrapped__()
                    else:
                        default_values[key] = val.default.arg
        return default_values

    def _get_model_onupdate_value(self, ) -> Dict:
        """
        获取update默认值
        Args:
        Returns:

        """
        update_values = {}
        for key, val in self._model.__dict__.items():
            if not key.startswith("_") and isinstance(val, InstrumentedAttribute):
                if val.onupdate and val.onupdate.is_callable:
                    update_values[key] = val.onupdate.arg.__wrapped__()
        return update_values

    @staticmethod
    def _base_params(query, dp, compiled, is_update) -> Optional[Dict]:
        """
        handle params
        """
        if dp and isinstance(dp, (list, tuple)):
            if is_update:
                dp = {c.key: pval for c, pval in zip(query.table.c, dp)}
            else:
                raise exc.ArgumentError("Don't mix sqlalchemy SELECT clause with positional parameters")
        compiled_params = compiled.construct_params(dp)
        processors = compiled._bind_processors
        params = [{
            key: processors.get(key, noop)(compiled_params[key])
            for key in compiled_params
        }]
        post_processed_params = _dialect.execute_sequence_format(params)
        return post_processed_params[0]

    def _compiled_quey(self, query: Union[Select, Insert, Update, Delete], *multiparams: Union[Dict, List[Dict]]
                       ) -> Dict[str, Union[str, Dict, List[Dict], None]]:
        """
        compile query to sql
        Args:
            query:
            multiparams:
        Returns:
            {"sql": sql, "params": params}
        """
        bind_params = _distill_params(multiparams, {})

        if len(bind_params) > 1:
            if isinstance(query, str):
                query_, params_ = query, bind_params
            else:
                compiled = query.compile(dialect=_dialect)
                query_ = str(compiled)
                params_ = []
                for bind_param in bind_params:
                    params_.append(self._base_params(query, bind_param, compiled, isinstance(query, UpdateBase)))
        else:
            if bind_params:
                bind_params = bind_params[0]

            if isinstance(query, str):
                query_, params_ = query, bind_params or None
            else:
                compiled = query.compile(dialect=_dialect)
                query_ = str(compiled)
                params_ = self._base_params(query, bind_params, compiled, isinstance(query, UpdateBase))
        # 处理自动增加的后缀
        if getattr(self._model, "__table_suffix__", None) is not None:
            query_ = query_.replace(getattr(self._model, "__table_suffix__"), "")

        return {"sql": query_, "params": params_}

    def _verify_model(self, ):
        """

        Args:

        Returns:

        """
        if self._model is None:
            raise FuncArgsError("Query 对象中缺少Model")

    def insert_from_query(self, column_names: List, query: 'Query') -> 'Query':
        """
        查询并且插入数据, ``INSERT...FROM SELECT`` statement.

        e.g.::
            sel = select([table1.c.a, table1.c.b]).where(table1.c.c > 5)
            ins = table2.insert().from_select(['a', 'b'], sel)
        Args:
            column_names: 字符串列名列表或者Column类列名列表
            query: Query类
        Returns:
            (插入的条数,插入的ID)
        """
        self._verify_model()
        try:
            self._query_obj = insert(self._model).from_select(column_names, query._query_obj)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            return self

    def insert_query(self, insert_data: Union[List[Dict], Dict]) -> 'Query':
        """
        insert query
        Args:
            insert_data: 值类型Dict or List[Dict]
        Returns:
            Select object
        """
        self._verify_model()
        try:
            insert_data_: Union[List[Dict], Dict]
            if isinstance(insert_data, dict):
                insert_data_ = {**self._get_model_default_value(), **insert_data}
                query = insert(self._model).values(insert_data_)
            else:
                insert_data_ = [{**self._get_model_default_value(), **one_data} for one_data in insert_data]
                query = insert(self._model).values(insert_data_[0])
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            self._query_obj, self._insert_data = query, insert_data_
            return self

    def update_query(self, update_data: Union[List[Dict], Dict]) -> 'Query':
        """
        update query

        eg: where(User.c.id == bindparam("id")).values({"name": bindparam("name")})
         await conn.execute(sql, [{"id": 1, "name": "t1"}, {"id": 2, "name": "t2"}]
        Args:
            update_data: 值类型Dict or List[Dict]
        Returns:
            返回更新的条数
        """
        self._verify_model()
        try:
            update_data_: Union[List[Dict], Dict]
            if isinstance(update_data, MutableMapping):
                update_data_ = {**self._get_model_onupdate_value(), **update_data}
                values_data = update_data_ if not self._bind_values else {
                    key: val for key, val in update_data_.items() if key in self._bind_values}
            else:
                update_data_ = [{**self._get_model_onupdate_value(), **one_data} for one_data in update_data]
                values_data = update_data_[0] if not self._bind_values else {
                    key: val for key, val in update_data_[0].items() if key in self._bind_values}

            query = update(self._model).values(values_data)
            for one_clause in self._whereclause:
                query = query.where(one_clause)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            self._query_obj, self._update_data = query, update_data_
            return self

    def delete_query(self, ) -> 'Query':
        """
        delete query
        Args:
        Returns:
            返回删除的条数
        """
        self._verify_model()
        try:
            query = delete(self._model)
            for one_clause in self._whereclause:
                query = query.where(one_clause)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            self._query_obj = query
            return self

    def select_query(self, is_count: bool = False) -> 'Query':
        """
        select query
        Args:
            is_count: 是否为数量查询
        Returns:
            返回匹配的数据或者None
        """
        try:
            if is_count is False:
                query = select([self._model] if not self._columns else self._columns)
                # 以下的查询只有普通查询才有，和查询数量么有关系
                if self._order_by:
                    query.append_order_by(*self._order_by)
                if self._columns:
                    query = query.with_only_columns(self._columns)
                if self._limit_clause is not None:
                    query = query.limit(self._limit_clause)
                if self._offset_clause is not None:
                    query = query.offset(self._offset_clause)
            else:
                query = select([func.count().label("count")]).select_from(self._model)
            # 以下的查询条件都会有
            if self._with_hint:
                query = query.with_hint(*self._with_hint)
            if self._whereclause:
                for one_clause in self._whereclause:
                    query.append_whereclause(one_clause)
            if self._group_by:
                query.append_group_by(*self._group_by)
                for one_clause in self._having:
                    query.append_having(one_clause)
            if self._distinct:
                query = query.distinct(*self._distinct)
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            if is_count is False:
                self._query_obj = query
            else:
                self._query_count_obj = query
            return self

    # noinspection DuplicatedCode
    def paginate_query(self, *, page: int = 1, per_page: int = 20,
                       primary_order: bool = True) -> 'Query':
        """
        If ``page`` or ``per_page`` are ``None``, they will be retrieved from
        the request query. If there is no request or they aren't in the
        query, they default to 1 and 20 respectively.

        目前是改造如果limit传递为0，则返回所有的数据，这样业务代码中就不用更改了

        Args:
            page: page is less than 1, or ``per_page`` is negative.
            per_page: page or per_page are not ints.
            primary_order: 默认启用主键ID排序的功能，在大数据查询时可以关闭此功能，在90%数据量不大的情况下可以加快分页的速度

            When ``error_out`` is ``False``, ``page`` and ``per_page`` default to
            1 and 20 respectively.

        Returns:

        """
        try:
            page = int(page)
        except (TypeError, ValueError):
            page = 1

        try:
            per_page = int(per_page)
        except (TypeError, ValueError):
            per_page = 20

        if page < 1:
            page = 1

        if per_page < 0:
            per_page = 20

        self._page, self._per_page = page, per_page

        try:
            # 如果per_page为0,则证明要获取所有的数据，否则还是通常的逻辑
            if per_page != 0:
                self._limit_clause = per_page
                self._offset_clause = (page - 1) * per_page
                # 如果分页获取的时候没有进行排序,并且model中有id字段,则增加用id字段的升序排序
                # 前提是默认id是主键,因为不排序会有混乱数据,所以从中间件直接解决,业务层不需要关心了
                # 如果业务层有排序了，则此处不再提供排序功能
                # 如果遇到大数据量的分页查询问题时，建议关闭此处，然后再基于已有的索引分页
                if primary_order is True and getattr(self._model, "id", None) is not None:
                    self.order_by(getattr(self._model, "id").asc())

            self.select_query()  # 生成select SQL
            self.select_query(is_count=True)  # 生成select count SQL
        except SQLAlchemyError as e:
            aelog.exception(e)
            raise QueryArgsError(message="Cloumn args error: {}".format(str(e)))
        else:
            return self

    def sql(self, ) -> Union[Dict[str, Union[str, Dict, List[Dict], None]],
                             List[Dict[str, Union[str, Dict, List[Dict], None]]]]:
        """
        generate sql

        可以生成如下SQL:
            1.insert sql
            2.update sql
            3.delete sql
            4.select sql
            5.select count sql
            6.paginate_sql
        Args:
        Returns:
            1. insert sql {"sql": "insert sql", "params": "insert data"}
            2. update sql {"sql": "update sql", "params": "update data"}
            3. delete sql {"sql": "delete sql", "params": "delete params"}
            4. select sql {"sql": "select sql", "params": "select data"}
            5. select count sql {"sql": "select sql", "params": "select data"}
            6. paginate_sql [{"sql": "select sql", "params": "select params"},
                            {"sql": "select count sql", "params": "select count params"}]
        """
        result_sql: Union[Dict[str, Union[str, Dict, List[Dict], None]],
                          List[Dict[str, Union[str, Dict, List[Dict], None]]]] = {}

        if self._query_obj is not None and self._query_count_obj is not None:
            select_sql = self._compiled_quey(self._query_obj)
            select_count_sql = self._compiled_quey(self._query_count_obj)
            result_sql = [select_sql, select_count_sql]
        elif self._query_obj is not None and self._insert_data is not None:
            result_sql = self._compiled_quey(self._query_obj, self._insert_data)
        elif self._query_obj is not None and self._update_data is not None:
            result_sql = self._compiled_quey(self._query_obj, self._update_data)
        elif self._query_count_obj is not None:
            result_sql = self._compiled_quey(self._query_count_obj)
        elif self._query_obj is not None:
            result_sql = self._compiled_quey(self._query_obj)

        return result_sql
