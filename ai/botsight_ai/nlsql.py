"""Questions in natural language -> SQL -> result (Botsight.md, 14.2).

Two independent defences, so a mistaken or manipulated LLM cannot read another project:
1. the SQL is parsed and checked before it runs (one SELECT, only the events table,
   no SETTINGS/FORMAT/table functions);
2. it runs as the ClickHouse user nl_p{project_id}: a row policy shows only the project's
   rows, the user may only SELECT from events, and the nl_readonly profile makes it
   read-only with limits on time, rows and memory.
"""

from __future__ import annotations

import datetime as dt
import decimal
import hashlib
import hmac
import ipaddress
import logging
import threading
import uuid
from collections.abc import Callable
from typing import Any

import sqlglot
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from sqlglot import exp

from . import metrics
from .llm import LLM, LLMError, parse_json_object

log = logging.getLogger(__name__)

ALLOWED_TABLES = frozenset({"events"})
CHARTS = ("number", "line", "bar", "table")
MAX_SQL = 4000
MAX_ROWS = 1000

# Functions that reach outside the table even without a FROM: files, network, processes.
DENIED_FUNCTIONS = frozenset(
    {
        "file",
        "url",
        "urlcluster",
        "remote",
        "remotesecure",
        "cluster",
        "clusterallreplicas",
        "s3",
        "s3cluster",
        "gcs",
        "azureblobstorage",
        "hdfs",
        "hdfscluster",
        "mysql",
        "postgresql",
        "mongodb",
        "redis",
        "sqlite",
        "jdbc",
        "odbc",
        "executable",
        "input",
        "generaterandom",
        "dictget",
        "dictgetordefault",
        "joinget",
        "currentuser",
        "getsetting",
    }
)

SCHEMA = """Таблица events (ClickHouse) — события Telegram-бота. Одна строка — одно событие.
Колонки:
- ts DateTime64(3, 'UTC') — время события
- type — тип события: 'update' (входящее сообщение или нажатие), 'step' (сработал хендлер),
  'outgoing' (запрос бота к Telegram), 'custom' (бизнес-событие), 'lifecycle' (запуск и остановка бота)
- name — подтип: для update — 'message', 'callback_query', 'my_chat_member'; для outgoing — метод
  ('sendMessage', 'sendDocument'); для custom — имя события ('purchase'); для lifecycle — 'bot_started', 'bot_stopped'
- user_id UInt64 — пользователь Telegram; chat_id Int64 — чат
- command — команда без '/', например 'start'; start_param — метка deep link из '/start <метка>' (источник трафика)
- callback_data — данные нажатой кнопки
- state — состояние FSM в момент апдейта; handler — хендлер ('module:function');
  state_from, state_to — состояние до и после хендлера (шаги сценария)
- handled UInt8 — нашёлся ли хендлер для апдейта; ok UInt8 — успешен ли запрос к Telegram
- error — тип ошибки ('bot_blocked', 'rate_limited', имя исключения хендлера); error_code — код ошибки Telegram
- duration_ms — время обработки; text_len — длина текста
- member_status — для my_chat_member: 'kicked' в личном чате означает, что пользователь заблокировал бота
- user_lang, is_premium, has_username — сведения о пользователе
- props_str Map(String, String), props_num Map(String, Float64) — свойства ручных событий, например props_num['amount']

Определения:
- активные пользователи (DAU) — uniq(user_id) по событиям type = 'update' за день
- источник пользователя — start_param его первого '/start': argMin(start_param, ts) по type = 'update' AND command = 'start', сгруппировано по user_id
- блокировки — error = 'bot_blocked' или (name = 'my_chat_member' AND member_status = 'kicked')"""

PROMPT = """Ты пишешь один SQL-запрос для ClickHouse, который отвечает на вопрос владельца Telegram-бота.
Правила:
- только SELECT (можно WITH) и только таблица events, никаких других таблиц и табличных функций;
- фильтр по проекту не нужен: в таблице видны только данные этого бота;
- время в UTC, «сегодня» — today(), «эта неделя» — toMonday(now()), группировка по дням — toDate(ts);
- не больше 1000 строк, понятные псевдонимы колонок на английском в snake_case;
- без SETTINGS, FORMAT, INTO OUTFILE.
Ответь JSON-объектом без пояснений: {"sql": "...", "chart": "number|line|bar|table", "title": "..."}.
chart: number — одно число, line — ряд по времени, bar — сравнение категорий, table — остальное.
title — короткая подпись к ответу на русском."""


class NLSQLError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def validate(sql: str) -> str:
    """Raises NLSQLError("invalid_sql") unless sql is one read-only query of events."""
    sql = sql.strip().rstrip(";").strip()
    if not sql or len(sql) > MAX_SQL:
        raise NLSQLError("invalid_sql", "empty or too long query")
    try:
        statements = [s for s in sqlglot.parse(sql, read="clickhouse") if s is not None]
    except sqlglot.errors.ParseError as exc:
        raise NLSQLError("invalid_sql", f"cannot parse the query: {str(exc)[:200]}") from exc
    if len(statements) != 1:
        raise NLSQLError("invalid_sql", "exactly one statement is allowed")
    tree = statements[0]
    if not isinstance(tree, exp.Select | exp.Union):
        raise NLSQLError("invalid_sql", "only SELECT queries are allowed")
    for node in tree.walk():
        if isinstance(node, exp.Select | exp.Union) and (
            node.args.get("settings") or node.args.get("format")
        ):
            raise NLSQLError("invalid_sql", "SETTINGS and FORMAT are not allowed")
        if isinstance(node, exp.Into):
            raise NLSQLError("invalid_sql", "INTO is not allowed")
    ctes = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    for table in tree.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            raise NLSQLError("invalid_sql", "table functions are not allowed")
        name, db = table.name.lower(), table.db.lower()
        if name in ctes and not db:
            continue
        if name not in ALLOWED_TABLES or db not in ("", "botsight"):
            raise NLSQLError("invalid_sql", f"table {table.sql()} is not allowed, only events")
    for fn in tree.find_all(exp.Func):
        name = (fn.name if isinstance(fn, exp.Anonymous) else fn.sql_name()).lower()
        if name in DENIED_FUNCTIONS:
            raise NLSQLError("invalid_sql", f"function {name} is not allowed")
    return sql


def _json_value(v: Any) -> Any:
    match v:
        case dt.datetime() | dt.date():
            return v.isoformat()
        case uuid.UUID() | ipaddress.IPv4Address() | ipaddress.IPv6Address():
            return str(v)
        case decimal.Decimal():
            return float(v)
        case bytes():
            return v.decode("utf-8", "replace")
        case float() if v != v or v in (float("inf"), float("-inf")):
            return None
        case list() | tuple():
            return [_json_value(x) for x in v]
        case dict():
            return {str(k): _json_value(x) for k, x in v.items()}
    return v


class Asker:
    def __init__(
        self,
        llm: LLM,
        admin: Callable[[], Client],
        as_user: Callable[[str, str], Client],
        password_secret: str,
        database: str = "botsight",
    ) -> None:
        self.llm = llm
        self.admin = admin
        self.as_user = as_user
        self.secret = password_secret.encode()
        self.database = database
        self._ensured: set[int] = set()
        self._lock = threading.Lock()

    def ask(self, project_id: int, question: str) -> dict[str, Any]:
        question = question.strip()
        if not question or len(question) > 500:
            raise NLSQLError("invalid_question", "a question is 1 to 500 characters")
        if not self.llm.enabled:
            metrics.ask.labels("llm_disabled").inc()
            raise NLSQLError("llm_disabled", "natural language questions need an LLM")
        client = self._client(project_id)
        try:
            context = self._context(client)
            reply = self.llm.complete(
                PROMPT,
                f"{SCHEMA}\n\nСловарь этого бота:\n{context}\n\nВопрос: {question}",
                purpose="nl_sql",
                max_tokens=600,
            )
            obj = parse_json_object(reply.text)
        except LLMError as exc:
            metrics.ask.labels("llm_error").inc()
            raise NLSQLError("llm_error", str(exc)) from exc
        chart = obj.get("chart") if obj.get("chart") in CHARTS else "table"
        try:
            sql = validate(str(obj.get("sql", "")))
        except NLSQLError:
            metrics.ask.labels("invalid_sql").inc()
            raise
        try:
            result = client.query(sql)
        except ClickHouseError as exc:
            metrics.ask.labels("query_failed").inc()
            raise NLSQLError("query_failed", str(exc).split("\n")[0][:300]) from exc
        rows = [[_json_value(v) for v in row] for row in result.result_rows[: MAX_ROWS + 1]]
        metrics.ask.labels("ok").inc()
        return {
            "question": question,
            "title": str(obj.get("title", ""))[:200],
            "chart": chart,
            "sql": sql,
            "columns": list(result.column_names),
            "rows": rows[:MAX_ROWS],
            "truncated": len(rows) > MAX_ROWS,
        }

    def _context(self, client: Client) -> str:
        """Names the LLM should use: FSM states, commands, manual events, sources (14.2)."""
        rows = client.query(
            """
            SELECT kind, v FROM (
                SELECT 'состояния FSM' AS kind, state_to AS v, count() AS c FROM events
                WHERE type = 'step' AND state_to != '' AND ts >= now() - INTERVAL 30 DAY GROUP BY v
                UNION ALL
                SELECT 'команды', command, count() FROM events
                WHERE type = 'update' AND command != '' AND ts >= now() - INTERVAL 30 DAY GROUP BY command
                UNION ALL
                SELECT 'ручные события (type = custom)', name, count() FROM events
                WHERE type = 'custom' AND ts >= now() - INTERVAL 30 DAY GROUP BY name
                UNION ALL
                SELECT 'метки источников (start_param)', start_param, count() FROM events
                WHERE command = 'start' AND start_param != '' AND ts >= now() - INTERVAL 30 DAY
                GROUP BY start_param
            ) ORDER BY kind, c DESC LIMIT 30 BY kind
            """
        ).result_rows
        by_kind: dict[str, list[str]] = {}
        for kind, v in rows:
            by_kind.setdefault(kind, []).append(str(v)[:64])
        return (
            "\n".join(f"- {k}: {', '.join(vs)}" for k, vs in by_kind.items()) or "- пока нет данных"
        )

    def _password(self, project_id: int) -> str:
        return hmac.new(self.secret, f"nl_p{project_id}".encode(), hashlib.sha256).hexdigest()

    def _client(self, project_id: int) -> Client:
        if project_id <= 0:
            raise NLSQLError("invalid_project", "invalid project")
        self._ensure_user(project_id)
        return self.as_user(f"nl_p{project_id}", self._password(project_id))

    def _ensure_user(self, project_id: int) -> None:
        """Creates the project's ClickHouse user on first use. Idempotent; also re-applies
        the password, so rotating AI_NL_PASSWORD_SECRET just works."""
        with self._lock:
            if project_id in self._ensured:
                return
            user, db = f"nl_p{int(project_id)}", self.database
            admin = self.admin()
            pw = self._password(project_id)  # hex, safe inside the literal
            admin.command(
                f"CREATE USER IF NOT EXISTS {user} IDENTIFIED WITH sha256_password BY '{pw}' "
                f"DEFAULT DATABASE {db} SETTINGS PROFILE 'nl_readonly'"
            )
            admin.command(f"ALTER USER {user} IDENTIFIED WITH sha256_password BY '{pw}'")
            admin.command(
                f"CREATE ROW POLICY IF NOT EXISTS {user} ON {db}.events FOR SELECT "
                f"USING project_id = {int(project_id)} TO {user}"
            )
            admin.command(f"GRANT SELECT ON {db}.events TO {user}")
            self._ensured.add(project_id)
