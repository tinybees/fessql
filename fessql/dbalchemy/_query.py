#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2021/3/19 下午6:50
"""
from math import ceil
from typing import List

from sqlalchemy import orm
# noinspection PyProtectedMember
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

    def iter_pages(self, left_edge=2, left_current=2,
                   right_current=5, right_edge=2):
        """Iterates over the page numbers in the pagination.  The four
        parameters control the thresholds how many numbers should be produced
        from the sides.  Skipped page numbers are represented as `None`.
        This is how you could render such a pagination in the templates:

        .. sourcecode:: html+jinja

            {% macro render_pagination(pagination, endpoint) %}
              <div class=pagination>
              {%- for page in pagination.iter_pages() %}
                {% if page %}
                  {% if page != pagination.page %}
                    <a href="{{ url_for(endpoint, page=page) }}">{{ page }}</a>
                  {% else %}
                    <strong>{{ page }}</strong>
                  {% endif %}
                {% else %}
                  <span class=ellipsis>…</span>
                {% endif %}
              {%- endfor %}
              </div>
            {% endmacro %}
        """
        last = 0
        for num in range(1, self.pages + 1):
            if (num <= left_edge
                    or (self.page - left_current - 1 < num < self.page + right_current)
                    or num > self.pages - right_edge):
                if last + 1 != num:
                    yield None
                yield num
                last = num


class FesQuery(orm.Query):
    """
    改造Query,使得符合业务中使用

    目前是改造如果limit传递为0，则返回所有的数据，这样业务代码中就不用更改了
    """

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
        return super().union(*q)

    def union_all(self, *q) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().union_all(*q)

    def intersect(self, *q) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().intersect(*q)

    def intersect_all(self, *q) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().intersect_all(*q)

    def except_(self, *q) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
        return super().except_(*q)

    def except_all(self, *q) -> 'FesQuery':
        """
        继承父类便于自动提示提示
        """
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
