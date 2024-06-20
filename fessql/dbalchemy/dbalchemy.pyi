from typing import Any, ContextManager, Dict, List, Optional, Sequence, Type, Union

from sqlalchemy import orm
# noinspection PyProtectedMember
from sqlalchemy.engine import Engine
from sqlalchemy.engine.result import RowProxy
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import DeclarativeMeta

from fessql._alchemy import AlchemyMixIn
from ._query import FesQuery


class FesSession(orm.Session):
    bind_key: Optional[str]
    is_closed: bool = False
    mgr_session: Optional['FesMgrSession']

    def __init__(self, autocommit: bool = ..., autoflush: bool = ..., expire_on_commit: bool = ...,
                 query_cls: Type[FesQuery] = ..., **options) -> None:
        super().__init__(autoflush, expire_on_commit, autocommit, query_cls)
        ...

    def query(self, *entities, **kwargs) -> FesQuery: ...

    def close(self) -> None: ...


class FesMgrSession:
    _scoped_session: orm.scoped_session
    bind_key: Optional[str]

    def __init__(self, scoped_session: orm.scoped_session, bind_key: Optional[str] = ...) -> None: ...

    def sessfes(self) -> FesSession: ...

    def query(self, *entities, **kwargs) -> FesQuery: ...

    def execute(self, query: Union[FesQuery, str], params: Optional[Dict[str, Any]] = ...) -> Optional[RowProxy]: ...

    def query_execute(self, query: Union[FesQuery, str], params: Optional[Dict[str, Any]] = ...,
                      size: Optional[int] = ...) -> Union[List[RowProxy], RowProxy, None]: ...


class DBAlchemy(AlchemyMixIn):
    Model: DeclarativeMeta  # 应该标记为 ClassVar[DeclarativeMeta] 但是标记后pycharm不会自动提示了
    app: Any
    # engine pool
    engine_pool: Dict[Optional[str], Engine]
    # session maker pool
    sessionmaker_pool: Dict[Optional[str], Union[orm.sessionmaker, orm.scoped_session]]
    dialect: str
    charset: str  # 连接编码
    binary_prefix: bool  # 是否增加二进制前缀连接URL中
    # default bind connection
    username: str
    passwd: Optional[str]
    host: str
    port: int
    dbname: str
    db_uri: URL
    # session and engine
    kwargs: Dict[str, Any]
    session_options: Dict[str, Any]
    engine_options: Dict[str, Any]
    # other binds
    fessql_binds: Dict[str, Dict]

    def __init__(self, app=..., *, username: str = ..., passwd: str = ..., host: str = ...,
                 port: int = ..., dbname: str = ..., dialect: str = ..., fessql_binds: Optional[Dict[str, Dict]] = ...,
                 session_options: Optional[Dict[str, Any]] = ..., engine_options: Optional[Dict[str, Any]] = ...,
                 **kwargs) -> None: ...

    def _set_session_opts(self) -> None: ...

    def _set_engine_opts(self) -> None: ...

    @staticmethod
    def _apply_engine_opts(configs: Dict[str, Any], options: Dict[str, Any]): ...

    def get_engine_url(self, db_name, *, username: str = ..., password: Optional[str] = ..., host: str = ...,
                       port: int = ...) -> URL: ...

    def init_app(self, app) -> None: ...

    def _init_app(self, config: Dict[str, Any]) -> None: ...

    def init_engine(self, *, username: str = ..., passwd: Optional[str] = ..., host: str = ..., port: int = ...,
                    dbname: str = ..., **kwargs) -> None: ...

    def close_connection(self) -> None: ...

    def _create_scoped_sessionmaker(self, bind: Engine) -> orm.scoped_session: ...

    def _create_sessionmaker(self, bind: Engine) -> orm.sessionmaker: ...

    @staticmethod
    def _create_engine(sa_url: Union[str, URL], engine_opts: Dict[str, Any]) -> Engine: ...

    def _create_pool_engine(self, bind_key: str) -> None: ...

    def _gen_sessionmaker(self, bind_key: Optional[str] = ...) -> orm.scoped_session: ...

    def ping_session(self, session: FesSession, reconnect: bool = ...) -> FesSession: ...

    def gen_session(self, bind_key: Optional[str] = ...) -> FesMgrSession: ...

    @property
    def session(self) -> FesMgrSession: ...

    @staticmethod
    def insert_context(session: FesMgrSession) -> ContextManager[FesSession]: ...

    @staticmethod
    def update_context(session: FesMgrSession) -> ContextManager[FesSession]: ...

    @staticmethod
    def delete_context(session: FesMgrSession) -> ContextManager[FesSession]: ...

    def verify_binds(self) -> None: ...

    def gen_model(self, model_cls: DeclarativeMeta, class_suffix: str = ..., table_suffix: str = ...,
                  table_name: Optional[str] = ..., field_mapping: Optional[Dict[str, str]] = ...,
                  fields: Optional[Sequence[str]] = ...): ...
