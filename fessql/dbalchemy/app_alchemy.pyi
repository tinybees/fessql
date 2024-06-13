from typing import Any, Dict, Optional

from .dbalchemy import DBAlchemy


class FastapiAlchemy(DBAlchemy):
    def __init__(self, app=..., *, username: str = ..., passwd: str = ..., host: str = ..., port: int = ...,
                 dbname: str = ..., dialect: str = ..., fessql_binds: Optional[Dict[str, Dict]] = ...,
                 session_options: Optional[Dict[str, Any]] = ..., engine_options: Optional[Dict[str, Any]] = ...,
                 **kwargs) -> None:
        super().__init__(app, username=username, passwd=passwd, host=host, port=port, dbname=dbname, dialect=dialect,
                         fessql_binds=fessql_binds, session_options=session_options, engine_options=engine_options,
                         **kwargs)
        ...


    def init_app(self, app) -> None: ...

    def _verify_fastapi_app(self) -> None: ...


class FlaskAlchemy(DBAlchemy):
    def __init__(self, app=..., *, username: str = ..., passwd: str = ..., host: str = ..., port: int = ...,
                 dbname: str = ..., dialect: str = ..., fessql_binds: Optional[Dict[str, Dict]] = ...,
                 session_options: Optional[Dict[str, Any]] = ..., engine_options: Optional[Dict[str, Any]] = ...,
                 **kwargs) -> None:
        super().__init__(app, username=username, passwd=passwd, host=host, port=port, dbname=dbname, dialect=dialect,
                         fessql_binds=fessql_binds, session_options=session_options, engine_options=engine_options,
                         **kwargs)
        ...


    def init_app(self, app): ...

    def _verify_flask_app(self) -> None: ...
