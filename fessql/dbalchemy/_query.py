#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2021/3/19 下午6:50
"""
from contextlib import contextmanager
from math import ceil
from typing import Generator, List

from sqlalchemy import orm
from sqlalchemy.engine.result import RowProxy
from sqlalchemy.sql.schema import Table

__all__ = ("FesPagination", "FesQuery",)


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

    def prev(self, primary_order: bool = True):
        """
        primary_order: 默认启用主键ID排序的功能，在大数据查询时可以关闭此功能，在90%数据量不大的情况下可以加快分页的速度
        Returns a :class:`Pagination` object for the previous page.
        """
        assert (
                self.query is not None
        ), "a query object is required for this method to work"
        return self.query.paginate(page=self.page - 1, per_page=self.per_page, primary_order=primary_order)

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

    def next(self, primary_order: bool = True):
        """
        primary_order: 默认启用主键ID排序的功能，在大数据查询时可以关闭此功能，在90%数据量不大的情况下可以加快分页的速度
        Returns a :class:`Pagination` object for the next page.
        """
        assert (
                self.query is not None
        ), "a query object is required for this method to work"
        return self.query.paginate(page=self.page + 1, per_page=self.per_page, primary_order=primary_order)

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
    """

    def __init__(self, entities, sessfes=None, mgr_session=None):
        """Construct a :class:`_query.Query` directly.

        E.g.::

            q = Query([User, Address], session=some_session)

        The above is equivalent to::

            q = some_session.query(User, Address)

        :param entities: a sequence of entities and/or SQL expressions.

        :param sessfes: a :class:`.Session` with which the
         :class:`_query.Query`
         will be associated.   Optional; a :class:`_query.Query`
         can be associated
         with a :class:`.Session` generatively via the
         :meth:`_query.Query.with_session` method as well.

         :param mgr_session: a instance of FesMgrSession object.
        """
        super().__init__(entities, sessfes)
        self.mgr_session = mgr_session
        self.other_sessions = []  # 包含其他FesQuery的中的session,只要用于union等的操作

    # noinspection DuplicatedCode
    def paginate(self, page: int = 1, per_page: int = 20, primary_order: bool = True) -> FesPagination:
        """Returns ``per_page`` items from page ``page``.

        If ``page`` or ``per_page`` are ``None``, they will be retrieved from
        the request query. If there is no request or they aren't in the
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
        # 判断是否关闭,关闭后赋予新的session
        if self.session.is_closed:
            self.with_session(self.mgr_session.sessfes())
        # 如果per_page为0,则证明要获取所有的数据,这里最大返回1000条数据，否则还是通常的逻辑
        if per_page != 0:
            items = self.limit(per_page).offset((page - 1) * per_page).all(False)
        else:
            items = self.limit(1000).all(False)

        # No need to count if we're on the first page and there are fewer
        # items than we expected.
        if page == 1 and len(items) < per_page:
            total = len(items)
        else:
            total = self.order_by(None).count(False)
        # 查询完后,关闭session
        self.session.close()

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

    def with_for_update(self, read=False, nowait=False, of=None, skip_locked=False, key_share=False) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().with_for_update(read, nowait, of, skip_locked, key_share)

    def with_labels(self) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().with_labels()

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
        self.other_sessions.extend([one_q.session for one_q in q if getattr(one_q, "session", None)])
        return super().union(*q)

    def union_all(self, *q) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        self.other_sessions.extend([one_q.session for one_q in q if getattr(one_q, "session", None)])
        return super().union_all(*q)

    def intersect(self, *q) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        self.other_sessions.extend([one_q.session for one_q in q if getattr(one_q, "session", None)])
        return super().intersect(*q)

    def intersect_all(self, *q) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        self.other_sessions.extend([one_q.session for one_q in q if getattr(one_q, "session", None)])
        return super().intersect_all(*q)

    def except_(self, *q) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        self.other_sessions.extend([one_q.session for one_q in q if getattr(one_q, "session", None)])
        return super().except_(*q)

    def except_all(self, *q) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        self.other_sessions.extend([one_q.session for one_q in q if getattr(one_q, "session", None)])
        return super().except_all(*q)

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

    def add_columns(self, *column) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().add_columns(*column)

    def join(self, *props, **kwargs) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().join(*props, **kwargs)

    def outerjoin(self, *props, **kwargs) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().outerjoin(*props, **kwargs)

    def correlate(self, *args) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().correlate(*args)

    def params(self, *args, **kwargs) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().params(*args, **kwargs)

    def prefix_with(self, *prefixes) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().prefix_with(*prefixes)

    def suffix_with(self, *suffixes) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().suffix_with(*suffixes)

    def subquery(self, name=None, with_labels=False, reduce_columns=False) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().subquery(name, with_labels, reduce_columns)

    def add_entity(self, entity, alias=None) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().add_entity(entity, alias)

    @contextmanager
    def close_session(self, is_closed: bool = True) -> Generator[None, None, None]:
        """
        查询完成后关闭FesQuery中的FesSession

        Args:
            is_closed: 是否关闭,默认true
        Returns:

        """
        try:
            yield
        finally:
            if is_closed:
                self.session.close()
                for other_session in self.other_sessions:
                    other_session.close()

    def first(self, is_closed: bool = True):
        """Return the first result of this ``Query`` or
        None if the result doesn't contain any row.

        first() applies a limit of one within the generated SQL, so that
        only one primary entity row is generated on the server side
        (note this may consist of multiple result rows if join-loaded
        collections are present).

        Calling :meth:`_query.Query.first`
        results in an execution of the underlying
        query.

        """
        self.limit(1)
        with self.close_session(is_closed):
            return super().first()

    def all(self, is_closed: bool = True):
        """Return the results represented by this :class:`_query.Query`
        as a list.

        This results in an execution of the underlying SQL statement.
        """
        with self.close_session(is_closed):
            return super().all()

    def count(self, is_closed: bool = True):
        """Return a count of rows this the SQL formed by this :class:`Query`
        would return.

        This generates the SQL for this Query as follows::

            SELECT count(1) AS count_1 FROM (
                SELECT <rest of query follows...>
            ) AS anon_1 """

        with self.close_session(is_closed):
            return super().count()

    def scalar(self, is_closed: bool = True):
        """Return the first element of the first result or None
        if no rows present.  If multiple rows are returned,
        raises MultipleResultsFound.

          >> session.query(Item).scalar()
          <Item>
          >> session.query(Item.id).scalar()
          1
          >> session.query(Item.id).filter(Item.id < 0).scalar()
          None
          >> session.query(Item.id, Item.name).scalar()
          1
          >> session.query(func.count(Parent.id)).scalar()
          20

        This results in an execution of the underlying query.

        """
        with self.close_session(is_closed):
            return super().scalar()

    def delete(self, synchronize_session=False) -> int:
        """Perform a bulk delete query.

        Deletes rows matched by this query from the database.

        E.g.::

            sess.query(User).filter(User.age == 25).\
                delete(synchronize_session=False)

            sess.query(User).filter(User.age == 25).\
                delete(synchronize_session='evaluate')

        :param synchronize_session: chooses the strategy for the removal of
        matched objects from the session. Valid values are:

        ``False`` - don't synchronize the session. This option is the most
        efficient and is reliable once the session is expired, which
        typically occurs after a commit(), or explicitly using
        expire_all(). Before the expiration, objects may still remain in
        the session which were in fact deleted which can lead to confusing
        results if they are accessed via get() or already loaded
        collections.

        ``'fetch'`` - performs a select query before the delete to find
        objects that are matched by the delete query and need to be
        removed from the session. Matched objects are removed from the
        session.

        ``'evaluate'`` - Evaluate the query's criteria in Python straight
        on the objects in the session. If evaluation of the criteria isn't
        implemented, an error is raised.

        The expression evaluator currently doesn't account for differing
        string collations between the database and Python.

        :return: the count of rows matched as returned by the database's
          "row count" feature.

        """
        return super().delete(synchronize_session)

    def update(self, values, synchronize_session=False, update_args=None) -> int:
        r"""Perform a bulk update query.

        Updates rows matched by this query in the database.

        E.g.::

            sess.query(User).filter(User.age == 25).\
                update({User.age: User.age - 10}, synchronize_session=False)

            sess.query(User).filter(User.age == 25).\
                update({"age": User.age - 10}, synchronize_session='evaluate')

        .. warning:: The :meth:`_query.Query.update`
           method is a "bulk" operation,
           which bypasses ORM unit-of-work automation in favor of greater
           performance.  **Please read all caveats and warnings below.**

        :param values: a dictionary with attributes names, or alternatively
         mapped attributes or SQL expressions, as keys, and literal
         values or sql expressions as values.   If :ref:`parameter-ordered
         mode <updates_order_parameters>` is desired, the values can be
         passed as a list of 2-tuples;
         this requires that the
         :paramref:`~sqlalchemy.sql.expression.update.preserve_parameter_order`
         flag is passed to the :paramref:`.Query.update.update_args` dictionary
         as well.

          .. versionchanged:: 1.0.0 - string names in the values dictionary
             are now resolved against the mapped entity; previously, these
             strings were passed as literal column names with no mapper-level
             translation.

        :param synchronize_session: chooses the strategy to update the
         attributes on objects in the session. Valid values are:

            ``False`` - don't synchronize the session. This option is the most
            efficient and is reliable once the session is expired, which
            typically occurs after a commit(), or explicitly using
            expire_all(). Before the expiration, updated objects may still
            remain in the session with stale values on their attributes, which
            can lead to confusing results.

            ``'fetch'`` - performs a select query before the update to find
            objects that are matched by the update query. The updated
            attributes are expired on matched objects.

            ``'evaluate'`` - Evaluate the Query's criteria in Python straight
            on the objects in the session. If evaluation of the criteria isn't
            implemented, an exception is raised.

            The expression evaluator currently doesn't account for differing
            string collations between the database and Python.

        :param update_args: Optional dictionary, if present will be passed
         to the underlying :func:`_expression.update`
         construct as the ``**kw`` for
         the object.  May be used to pass dialect-specific arguments such
         as ``mysql_limit``, as well as other special arguments such as
         :paramref:`~sqlalchemy.sql.expression.update.preserve_parameter_order`.

         .. versionadded:: 1.0.0

        :return: the count of rows matched as returned by the database's
         "row count" feature.

        """
        return super().update(values, synchronize_session, update_args)
