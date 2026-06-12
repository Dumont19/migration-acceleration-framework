"""
services/dimension/oracle_fetcher.py
--------------------------------------
Busca metadados Oracle (colunas + constraints) para uma tabela de dimensão.

REGRA ABSOLUTA: colunas para DDL do dim_migration vêm SEMPRE do Oracle,
nunca do XML DataStage. Este módulo implementa essa regra.

Funções:
  fetch_dim_columns     → lista de OracleColumnInfo (all_tab_columns)
  fetch_dim_constraints → lista de OracleConstraintInfo (all_constraints)
"""
from __future__ import annotations

from app.core.oracle_client import get_oracle_pool

from .schemas import OracleColumnInfo, OracleConstraintInfo

_COLS_SQL = """
SELECT
    c.column_name,
    c.data_type,
    c.data_length,
    c.data_precision,
    c.data_scale,
    c.nullable,
    cm.comments
FROM all_tab_columns c
LEFT JOIN all_col_comments cm
    ON  cm.owner       = c.owner
    AND cm.table_name  = c.table_name
    AND cm.column_name = c.column_name
WHERE c.owner      = :owner
  AND c.table_name = :table_name
ORDER BY c.column_id
"""

_CONS_SQL = """
SELECT
    cc.column_name,
    c.constraint_type,
    c.constraint_name
FROM all_constraints c
JOIN all_cons_columns cc
    ON  cc.owner           = c.owner
    AND cc.constraint_name = c.constraint_name
WHERE c.owner      = :owner
  AND c.table_name = :table_name
  AND c.constraint_type IN ('P', 'U', 'C')
ORDER BY c.constraint_type, cc.position
"""


async def fetch_dim_columns(owner: str, table_name: str) -> list[OracleColumnInfo]:
    """Busca colunas da tabela Oracle e retorna em ordem de column_id.

    Args:
        owner: Schema Oracle (ex: 'DWADM'). Convertido para maiúsculas.
        table_name: Nome da tabela. Convertido para maiúsculas.

    Returns:
        Lista de OracleColumnInfo na ordem do catálogo Oracle.

    Raises:
        RuntimeError: Se o pool Oracle não estiver inicializado.
        Exception: Qualquer erro de conectividade Oracle.
    """
    pool = get_oracle_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                _COLS_SQL,
                owner=owner.upper(),
                table_name=table_name.upper(),
            )
            rows = await cur.fetchall()
    return [
        OracleColumnInfo(
            name=r[0],
            data_type=r[1],
            data_length=r[2],
            data_precision=r[3],
            data_scale=r[4],
            nullable=(r[5] == "Y"),
            comment=r[6],
        )
        for r in rows
    ]


async def fetch_dim_constraints(owner: str, table_name: str) -> list[OracleConstraintInfo]:
    """Busca constraints da tabela Oracle (PK, UNIQUE, CHECK).

    Args:
        owner: Schema Oracle (ex: 'DWADM'). Convertido para maiúsculas.
        table_name: Nome da tabela. Convertido para maiúsculas.

    Returns:
        Lista de OracleConstraintInfo.

    Raises:
        RuntimeError: Se o pool Oracle não estiver inicializado.
        Exception: Qualquer erro de conectividade Oracle.
    """
    pool = get_oracle_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                _CONS_SQL,
                owner=owner.upper(),
                table_name=table_name.upper(),
            )
            rows = await cur.fetchall()
    return [
        OracleConstraintInfo(
            column_name=r[0],
            constraint_type=r[1],
            constraint_name=r[2],
        )
        for r in rows
    ]
