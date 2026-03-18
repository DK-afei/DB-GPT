"""Dameng(DM) connector using dmPython (DB-API).

This implementation is intentionally lightweight:
- Avoids requiring a SQLAlchemy DM dialect (often unavailable by default).
- Provides the minimum metadata APIs DB-GPT expects for business datasources.

Notes:
- DM is largely Oracle-compatible at the system view level. We use USER_* views.
- "database" here is treated as the schema/user's default namespace in DM.
"""

from __future__ import annotations

import logging
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type

from dbgpt.core.awel.flow import (
    TAGS_ORDER_HIGH,
    ResourceCategory,
    auto_register_resource,
)
from dbgpt.datasource.base import BaseConnector
from dbgpt.datasource.parameter import BaseDatasourceParameters
from dbgpt.util.i18n_utils import _

logger = logging.getLogger(__name__)


def _decode_if_bytes(v: Any) -> Any:
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8")
        except Exception:
            try:
                return v.decode("gbk")
            except Exception:
                return v.decode(errors="ignore")
    return v


@auto_register_resource(
    label=_("Dameng(DM) datasource"),
    category=ResourceCategory.DATABASE,
    tags={"order": TAGS_ORDER_HIGH},
    description=_("Dameng(DM) relational database datasource (dmPython driver)."),
)
@dataclass
class DMParameters(BaseDatasourceParameters):
    """DM connection parameters."""

    __type__ = "dm"

    host: str = field(metadata={"help": _("Database host, e.g., localhost")})
    user: str = field(metadata={"help": _("Database user to connect")})
    port: int = field(default=5236, metadata={"help": _("Database port, default 5236")})
    database: str = field(
        default="",
        metadata={
            "help": _(
                "Database/schema name. For DM, it is commonly used as the default schema."
            )
        },
    )
    password: str = field(
        default="${env:DBGPT_DB_PASSWORD}",
        metadata={
            "help": _(
                "Database password, you can write your password directly, of course, "
                "you can also use environment variables, such as ${env:DBGPT_DB_PASSWORD}"
            ),
            "tags": "privacy",
        },
    )
    connect_args: str = field(
        default="{}",
        metadata={
            "help": _(
                "Extra connect args passed to dmPython.connect() as a JSON string, "
                'e.g. {"compress": true}.'
            )
        },
    )

    def create_connector(self) -> "DMConnector":
        return DMConnector.from_parameters(self)

    def db_url(self, ssl: bool = False, charset: Optional[str] = None) -> str:
        # Only used for display/logging in most places.
        db_part = f"/{self.database}" if self.database else ""
        return f"dm://{self.user}:******@{self.host}:{self.port}{db_part}"


class DMConnector(BaseConnector):
    """Dameng(DM) connector."""

    db_type: str = "dm"
    driver: str = "dmpython"

    def __init__(self, conn, database: str = ""):
        self._conn = conn
        self._database = database or ""

    @classmethod
    def param_class(cls) -> Type[DMParameters]:
        return DMParameters

    @classmethod
    def from_parameters(cls, parameters: DMParameters) -> "DMConnector":
        try:
            import dmPython  # type: ignore
        except Exception as e:  # pragma: no cover
            raise ImportError(
                "dmPython is required for DM datasource. Please `pip install dmPython`."
            ) from e

        # dmPython connection API differs across distributions; try a few common forms.
        conn = None
        last_err: Optional[Exception] = None

        connect_args: Dict[str, Any] = {}
        if parameters.connect_args:
            try:
                connect_args = json.loads(parameters.connect_args)
                if not isinstance(connect_args, dict):
                    raise ValueError("connect_args must be a JSON object")
            except Exception:
                logger.warning("DM: invalid connect_args JSON, ignored.")
                connect_args = {}
        # If user provides explicit charset/encoding, pass through; otherwise keep dmPython defaults.

        for attempt in (
            lambda: dmPython.connect(  # type: ignore[attr-defined]
                user=parameters.user,
                password=parameters.password,
                server=parameters.host,
                port=parameters.port,
                **connect_args,
            ),
            lambda: dmPython.connect(  # type: ignore[attr-defined]
                user=parameters.user,
                password=parameters.password,
                host=parameters.host,
                port=parameters.port,
                **connect_args,
            ),
            lambda: dmPython.connect(  # type: ignore[attr-defined]
                f"{parameters.user}/{parameters.password}@{parameters.host}:{parameters.port}",
                **connect_args,
            ),
        ):
            try:
                conn = attempt()
                break
            except Exception as e:  # pragma: no cover
                last_err = e
                conn = None

        if conn is None:  # pragma: no cover
            raise ConnectionError(f"Failed to connect to DM: {last_err}") from last_err

        try:
            # Autocommit for safer interactive usage; DM defaults can vary.
            conn.autocommit = True  # type: ignore[attr-defined]
        except Exception:
            pass

        # Try to set default schema if provided.
        if parameters.database:
            try:
                cur = conn.cursor()
                cur.execute(f"SET SCHEMA {parameters.database}")
                cur.close()
            except Exception:
                # Non-fatal; many DM deployments use USER schema implicitly.
                logger.debug("DM: failed to SET SCHEMA, continue without it.")

        return cls(conn, database=parameters.database)

    @classmethod
    def from_uri_db(
        cls,
        host: str,
        port: int,
        user: str,
        pwd: str,
        db_name: str,
        engine_args: Optional[dict] = None,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> "DMConnector":
        """Create DMConnector from host/port/user/pwd/db_name.

        Notes:
        - DB-GPT's ConnectorManager will call this method for URL-style datasources.
        - `schema` is passed via ext_config in some flows; for DM we treat it as the
          default schema if provided.
        - `engine_args` is ignored (kept for signature compatibility).
        """
        try:
            import dmPython  # type: ignore
        except Exception as e:  # pragma: no cover
            raise ImportError(
                "dmPython is required for DM datasource. Please `pip install dmPython`."
            ) from e

        conn = None
        last_err: Optional[Exception] = None

        # Allow passing extra dmPython kwargs through `connect_args` in kwargs.
        connect_args = kwargs.get("connect_args") or {}
        if isinstance(connect_args, str):
            try:
                connect_args = json.loads(connect_args)
            except Exception:
                connect_args = {}
        if not isinstance(connect_args, dict):
            connect_args = {}

        for attempt in (
            lambda: dmPython.connect(  # type: ignore[attr-defined]
                user=user,
                password=pwd,
                server=host,
                port=port,
                **connect_args,
            ),
            lambda: dmPython.connect(  # type: ignore[attr-defined]
                user=user,
                password=pwd,
                host=host,
                port=port,
                **connect_args,
            ),
            lambda: dmPython.connect(  # type: ignore[attr-defined]
                f"{user}/{pwd}@{host}:{port}",
                **connect_args,
            ),
        ):
            try:
                conn = attempt()
                break
            except Exception as e:  # pragma: no cover
                last_err = e
                conn = None

        if conn is None:  # pragma: no cover
            raise ConnectionError(f"Failed to connect to DM: {last_err}") from last_err

        try:
            conn.autocommit = True  # type: ignore[attr-defined]
        except Exception:
            pass

        # Prefer explicit schema; fallback to db_name when it looks like a schema.
        default_schema = schema or db_name
        if default_schema:
            try:
                cur = conn.cursor()
                cur.execute(f"SET SCHEMA {default_schema}")
                cur.close()
            except Exception:
                logger.debug("DM: failed to SET SCHEMA in from_uri_db().")

        return cls(conn, database=default_schema)

    @property
    def db_url(self) -> str:
        return "dm://"

    @property
    def dialect(self) -> str:
        """Dialect name used for prompt/SQL generation."""
        # DM is largely Oracle-compatible at SQL dialect level.
        # return "oracle"
        return "dm"

    def close(self):
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass

    def _execute(self, sql: str, params: Optional[Dict[str, Any]] = None):
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params or {})
            return cur
        except Exception:
            cur.close()
            raise

    def run(self, command: str, fetch: str = "all") -> List:
        """Execute SQL and return results in DB-GPT expected list format.

        The first row is the tuple of column names when rows are returned.
        """
        if not command:
            return []
        sql = command.strip()
        cur = self._execute(sql)
        try:
            if cur.description:
                cols = tuple(d[0] for d in cur.description)
                if fetch == "one":
                    row = cur.fetchone()
                    rows = [] if row is None else [row]
                else:
                    rows = cur.fetchall()
                result = [cols]
                result.extend(rows)
                return result
            return []
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def run_to_df(self, command: str, fetch: str = "all"):
        """Execute SQL and return a pandas DataFrame.

        Some chat-db flows (auto_execute out_parser) require this method to generate
        view/chart payloads.
        """
        import pandas as pd

        result = self.run(command, fetch=fetch)
        if not result:
            return pd.DataFrame()
        columns = list(result[0]) if result and isinstance(result[0], (list, tuple)) else []
        rows = result[1:] if len(result) > 1 else []
        return pd.DataFrame(rows, columns=columns)

    def get_current_db_name(self) -> str:
        return self._database or self.user_schema()

    def user_schema(self) -> str:
        cur = None
        try:
            cur = self._execute("SELECT USER FROM dual")
            row = cur.fetchone()
            return str(_decode_if_bytes(row[0])) if row else ""
        finally:
            try:
                if cur:
                    cur.close()
            except Exception:
                pass

    def get_database_names(self) -> List[str]:
        # DM doesn't have "databases" like MySQL; return schemas/users as a pragmatic list.
        cur = None
        try:
            cur = self._execute("SELECT USERNAME FROM ALL_USERS")
            return [str(_decode_if_bytes(r[0])) for r in cur.fetchall()]
        except Exception:
            # Fallback: at least return current schema.
            return [self.get_current_db_name()]
        finally:
            try:
                if cur:
                    cur.close()
            except Exception:
                pass

    def get_table_names(self) -> Iterable[str]:
        tables: List[str] = []
        cur = None
        try:
            cur = self._execute("SELECT TABLE_NAME FROM USER_TABLES")
            tables.extend([str(_decode_if_bytes(r[0])) for r in cur.fetchall()])
            cur.close()
        except Exception:
            pass

        # Include views as well.
        try:
            cur = self._execute("SELECT VIEW_NAME FROM USER_VIEWS")
            tables.extend([str(_decode_if_bytes(r[0])) for r in cur.fetchall()])
        except Exception:
            pass
        finally:
            try:
                if cur:
                    cur.close()
            except Exception:
                pass

        return tables

    def get_fields(self, table_name: str) -> List[Tuple]:
        tn = table_name.upper()
        sql = f"""
            SELECT col.column_name,
                   col.data_type,
                   col.data_default,
                   col.nullable,
                   comm.comments
              FROM user_tab_columns col
              LEFT JOIN user_col_comments comm
                ON col.table_name = comm.table_name
               AND col.column_name = comm.column_name
             WHERE col.table_name = '{tn}'
             ORDER BY col.column_id
        """
        cur = self._execute(sql)
        try:
            rows = cur.fetchall()
            return [
                (
                    _decode_if_bytes(r[0]),
                    _decode_if_bytes(r[1]),
                    _decode_if_bytes(r[2]),
                    _decode_if_bytes(r[3]),
                    _decode_if_bytes(r[4]),
                )
                for r in rows
            ]
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def get_simple_fields(self, table_name: str) -> List[Tuple]:
        return self.get_fields(table_name)

    def get_columns(self, table_name: str) -> List[Dict]:
        fields = self.get_fields(table_name)
        cols = []
        for name, col_type, default, nullable, comment in fields:
            cols.append(
                {
                    "name": str(name),
                    "type": str(col_type),
                    "default_expression": default,
                    "nullable": str(nullable).upper() in ("Y", "YES", "TRUE", "1"),
                    "comment": comment,
                }
            )
        return cols

    def get_indexes(self, table_name: str) -> List[Dict]:
        """Return indexes for a table.

        DB-GPT expects a list of dicts like:
        [{"name": "...", "column_names": ["c1","c2"], "unique": bool}, ...]
        """
        tn = table_name.upper()
        sql = f"""
            SELECT ic.index_name,
                   ic.column_name,
                   ix.uniqueness
              FROM user_ind_columns ic
              JOIN user_indexes ix
                ON ic.index_name = ix.index_name
             WHERE ic.table_name = '{tn}'
             ORDER BY ic.index_name, ic.column_position
        """
        cur = self._execute(sql)
        try:
            rows = cur.fetchall()
        finally:
            try:
                cur.close()
            except Exception:
                pass

        idx: Dict[str, Dict] = {}
        for index_name, column_name, uniqueness in rows:
            index_name = str(_decode_if_bytes(index_name))
            column_name = str(_decode_if_bytes(column_name))
            uniq = str(_decode_if_bytes(uniqueness or "")).upper() == "UNIQUE"
            if index_name not in idx:
                idx[index_name] = {
                    "name": index_name,
                    "column_names": [],
                    "unique": uniq,
                }
            idx[index_name]["column_names"].append(column_name)
        return list(idx.values())

    def get_table_comment(self, table_name: str) -> Dict:
        tn = table_name.upper()
        cur = self._execute(
            f"SELECT comments FROM user_tab_comments WHERE table_name = '{tn}'"
        )
        try:
            row = cur.fetchone()
            return {"text": _decode_if_bytes(row[0]) if row else ""}
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def get_table_comments(self, db_name: str) -> List[Tuple[str, str]]:
        cur = self._execute("SELECT table_name, comments FROM user_tab_comments")
        try:
            return [
                (str(_decode_if_bytes(r[0])), str(_decode_if_bytes(r[1] or "")))
                for r in cur.fetchall()
            ]
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def get_column_comments(self, db_name: str, table_name: str):
        tn = table_name.upper()
        cur = self._execute(
            f"SELECT column_name, comments FROM user_col_comments WHERE table_name = '{tn}'"
        )
        try:
            return [
                (str(_decode_if_bytes(r[0])), str(_decode_if_bytes(r[1] or "")))
                for r in cur.fetchall()
            ]
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def get_charset(self) -> str:
        # DM is generally configured at database level; use Oracle-like NLS parameters.
        try:
            cur = self._execute(
                "SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET'"
            )
            row = cur.fetchone()
            return str(_decode_if_bytes(row[0])) if row else "UTF-8"
        except Exception:
            return "UTF-8"
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def get_collation(self) -> Optional[str]:
        try:
            cur = self._execute(
                "SELECT value FROM NLS_DATABASE_PARAMETERS WHERE parameter = 'NLS_SORT'"
            )
            row = cur.fetchone()
            return str(_decode_if_bytes(row[0])) if row else None
        except Exception:
            return None
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def get_table_info(self, table_names: Optional[List[str]] = None) -> str:
        # Lightweight "CREATE TABLE"-like output built from USER_TAB_COLUMNS.
        names = list(table_names) if table_names else list(self.get_table_names())
        chunks: List[str] = []
        for t in names:
            fields = self.get_fields(t)
            if not fields:
                continue
            lines = []
            for name, col_type, default, nullable, _comment in fields:
                line = f"  {name} {col_type}"
                if default not in (None, "", "NULL"):
                    line += f" DEFAULT {default}"
                if str(nullable).upper() in ("N", "NO", "FALSE", "0"):
                    line += " NOT NULL"
                lines.append(line)
            chunks.append(f"CREATE TABLE {t} (\n" + ",\n".join(lines) + "\n);")
        return "\n\n".join(chunks)

