"""Schema payloads shaped like what the gateway hands to on_schema()."""

from __future__ import annotations

from vizai_connector_gateway import SchemaColumn, SchemaForeignKey, SchemaPayload, SchemaTable


def mall_schema(connector_id: str = "c", *, schema_hash: str = "hash-1", dialect: str = "postgres") -> SchemaPayload:
    return SchemaPayload(
        connector_id=connector_id,
        instance_id="i1",
        dialect=dialect,
        schema_name="public",
        schema_hash=schema_hash,
        tables=(
            SchemaTable(
                name="malls",
                schema="public",
                kind="table",
                columns=(
                    SchemaColumn("mall_id", "integer", nullable=False),
                    SchemaColumn("name", "character varying(120)"),
                    SchemaColumn("status", "mall_status", enum_values=("open", "closed")),
                ),
                primary_key=("mall_id",),
            ),
            SchemaTable(
                name="stores",
                schema="public",
                kind="table",
                columns=(
                    SchemaColumn("store_id", "integer", nullable=False),
                    SchemaColumn("mall_id", "integer"),
                    SchemaColumn("rent", "numeric(12,2)"),
                    SchemaColumn("opened_at", "timestamp with time zone"),
                ),
                primary_key=("store_id",),
                foreign_keys=(SchemaForeignKey("mall_id", "malls", "mall_id"),),
            ),
            SchemaTable(
                name="v_mall_revenue",
                schema="public",
                kind="view",
                columns=(SchemaColumn("mall_id", "integer"), SchemaColumn("revenue", "numeric")),
            ),
        ),
        min_date="2023-01-01",
        max_date="2025-06-30",
    )
