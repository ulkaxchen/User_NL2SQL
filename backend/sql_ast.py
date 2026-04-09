from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable


def quote_identifier(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        raise ValueError("identifier 不能为空")
    return "`" + text.replace("`", "``") + "`"


def quote_literal(value: object) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    return "'" + text.replace("'", "''") + "'"


class SqlNode:
    def render(self) -> str:
        raise NotImplementedError

    def __str__(self) -> str:
        return self.render()


@dataclass(frozen=True)
class RawExpr(SqlNode):
    sql: str

    def render(self) -> str:
        return str(self.sql or "").strip()


@dataclass(frozen=True)
class ColumnRef(SqlNode):
    name: str
    table: str | None = None

    def render(self) -> str:
        if self.table:
            return f"{quote_identifier(self.table)}.{quote_identifier(self.name)}"
        return quote_identifier(self.name)


@dataclass(frozen=True)
class BinaryPredicate(SqlNode):
    left: SqlNode | str
    op: str
    right: SqlNode | str

    def render(self) -> str:
        lft = self.left.render() if isinstance(self.left, SqlNode) else str(self.left)
        rgt = self.right.render() if isinstance(self.right, SqlNode) else str(self.right)
        return f"{lft} {self.op} {rgt}"


@dataclass(frozen=True)
class LikePredicate(SqlNode):
    expr: SqlNode | str
    pattern: str
    lower: bool = False

    def render(self) -> str:
        target = self.expr.render() if isinstance(self.expr, SqlNode) else str(self.expr)
        if self.lower:
            target = f"LOWER({target})"
            value = self.pattern.lower()
        else:
            value = self.pattern
        return f"{target} LIKE {quote_literal(value)}"


@dataclass(frozen=True)
class CompoundPredicate(SqlNode):
    op: str
    children: tuple[SqlNode | str, ...]

    def render(self) -> str:
        rendered = []
        for child in self.children:
            sql = child.render() if isinstance(child, SqlNode) else str(child).strip()
            if not sql:
                continue
            rendered.append(f"({sql})")
        if not rendered:
            return "1=1"
        if len(rendered) == 1:
            return rendered[0]
        sep = f" {self.op} "
        return sep.join(rendered)


@dataclass(frozen=True)
class JoinClause(SqlNode):
    table: str
    on: SqlNode | str
    join_type: str = "INNER"

    def render(self) -> str:
        on_sql = self.on.render() if isinstance(self.on, SqlNode) else str(self.on).strip()
        return f"{self.join_type} JOIN {quote_identifier(self.table)} ON {on_sql}"


@dataclass(frozen=True)
class SelectItem(SqlNode):
    expr: SqlNode | str
    alias: str | None = None

    def render(self) -> str:
        sql = self.expr.render() if isinstance(self.expr, SqlNode) else str(self.expr).strip()
        if self.alias:
            return f"{sql} AS {quote_identifier(self.alias)}"
        return sql


@dataclass
class SelectQuery(SqlNode):
    from_table: str
    select_items: list[SelectItem] = field(default_factory=list)
    where: SqlNode | str | None = None
    joins: list[JoinClause] = field(default_factory=list)
    group_by: list[SqlNode | str] = field(default_factory=list)
    order_by: list[str] = field(default_factory=list)
    limit: int | None = None

    def render(self) -> str:
        if not self.select_items:
            raise ValueError("select_items 不能为空")
        parts = [
            "SELECT " + ", ".join(item.render() for item in self.select_items),
            "FROM " + quote_identifier(self.from_table),
        ]
        for join in self.joins:
            parts.append(join.render())
        if self.where is not None:
            where_sql = self.where.render() if isinstance(self.where, SqlNode) else str(self.where).strip()
            if where_sql:
                parts.append("WHERE " + where_sql)
        if self.group_by:
            gb = []
            for item in self.group_by:
                gb.append(item.render() if isinstance(item, SqlNode) else str(item).strip())
            parts.append("GROUP BY " + ", ".join(x for x in gb if x))
        if self.order_by:
            parts.append("ORDER BY " + ", ".join(str(x).strip() for x in self.order_by if str(x).strip()))
        if self.limit is not None:
            parts.append(f"LIMIT {int(self.limit)}")
        return "\n".join(parts)


def AND(*children: SqlNode | str | None) -> CompoundPredicate:
    vals = tuple(child for child in children if child is not None and str(child.render() if isinstance(child, SqlNode) else child).strip())
    return CompoundPredicate("AND", vals)


def OR(*children: SqlNode | str | None) -> CompoundPredicate:
    vals = tuple(child for child in children if child is not None and str(child.render() if isinstance(child, SqlNode) else child).strip())
    return CompoundPredicate("OR", vals)


def raw(sql: str) -> RawExpr:
    return RawExpr(sql)


def count_select(table: str, expression: str = "COUNT(*)", alias: str = "cnt", where: SqlNode | str | None = None, joins: Iterable[JoinClause] | None = None) -> SelectQuery:
    return SelectQuery(
        from_table=table,
        select_items=[SelectItem(raw(expression), alias)],
        where=where,
        joins=list(joins or []),
    )
