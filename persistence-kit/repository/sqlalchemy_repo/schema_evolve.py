from dataclasses import fields as dc_fields
from sqlalchemy import inspect, text
from sqlalchemy.dialects import postgresql
from persistence_kit.repository.sqlalchemy_repo.table_factory import _unwrap_optional, _sa_type

def ensure_missing_columns(sync_conn, table, entity_type):
    inspector = inspect(sync_conn)
    existing = {c["name"] for c in inspector.get_columns(table.name)}
    for f in dc_fields(entity_type):
        col = f.name
        if col in existing:
            continue
        ft, is_optional = _unwrap_optional(f.type)
        sa_type = _sa_type(ft)
        dtype = sa_type() if callable(sa_type) else sa_type
        ddl = dtype.compile(dialect=postgresql.dialect())
        nullable = "NULL" if is_optional else "NOT NULL"
        sync_conn.execute(text(f'ALTER TABLE "{table.name}" ADD COLUMN "{col}" {ddl} {nullable}'))

def _constraint_exists(sync_conn, table_name: str, constraint_name: str) -> bool:
    q = text("""
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = :t AND constraint_name = :c
        LIMIT 1
    """)
    return bool(sync_conn.execute(q, {"t": table_name, "c": constraint_name}).scalar())

def ensure_foreign_keys(sync_conn, table, fk_map: dict[str, tuple[str, str]]) -> None:
    for local_col, (target_tbl, target_col) in fk_map.items():
        cname = f'fk_{table.name}_{local_col}__{target_tbl}_{target_col}'
        if _constraint_exists(sync_conn, table.name, cname):
            continue
        sync_conn.execute(text(f'CREATE INDEX IF NOT EXISTS "idx_{table.name}_{local_col}" ON "{table.name}" ("{local_col}")'))
        sync_conn.execute(text(
            f'ALTER TABLE "{table.name}" '
            f'ADD CONSTRAINT "{cname}" FOREIGN KEY ("{local_col}") '
            f'REFERENCES "{target_tbl}" ("{target_col}") '
            f'ON UPDATE NO ACTION ON DELETE RESTRICT'
        ))
