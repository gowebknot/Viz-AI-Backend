"""The mapper's output is a contract with get_schema_structure (CLAUDE.md invariant 5)."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from app.connector_integration import schema_mapper
from app.connector_integration.schema_mapper import normalize_type

from .payloads import mall_schema

TABLE_KEYS = {"name", "catalog", "schema", "table", "columns", "primary_keys", "foreign_keys"}


def test_shape_matches_the_direct_flow():
    out = schema_mapper.map_schema(mall_schema())
    assert set(out) == {"tables", "min_date", "max_date"}
    assert [t["name"] for t in out["tables"]] == ["malls", "stores", "v_mall_revenue"]
    for table in out["tables"]:
        assert set(table) == TABLE_KEYS
        assert table["catalog"] is None and table["schema"] is None  # as for PostgreSQL in the direct flow
        assert table["name"] == table["table"]
        for column in table["columns"]:
            assert set(column) <= {"name", "type", "enum_values"}
            assert isinstance(column["type"], str)
        assert list(table["primary_keys"]) == ["constrained_columns"]
    json.dumps(out)  # it is saved as JSON text


def test_primary_and_foreign_keys_use_the_direct_flow_keys():
    malls, stores, _ = schema_mapper.map_schema(mall_schema())["tables"]
    assert malls["primary_keys"] == {"constrained_columns": ["mall_id"]}
    assert malls["foreign_keys"] == []
    assert stores["foreign_keys"] == [{"column": "mall_id", "references": "malls"}]


def test_enum_values_only_where_the_column_has_them():
    malls = schema_mapper.map_schema(mall_schema())["tables"][0]
    by_name = {c["name"]: c for c in malls["columns"]}
    assert by_name["status"]["enum_values"] == ["open", "closed"]
    assert "enum_values" not in by_name["name"]


def test_dates():
    out = schema_mapper.map_schema(mall_schema())
    assert (out["min_date"], out["max_date"]) == ("2023-01-01", "2025-06-30")
    odd = schema_mapper.map_schema(replace(mall_schema(), min_date="yesterday", max_date=None))
    assert (odd["min_date"], odd["max_date"]) == (None, None)


GOLDEN = json.loads((Path(__file__).parent / "golden_types.json").read_text())["types"]
# Types the direct flow cannot name: it writes NULL (xml, point) or DOMAIN. We keep the real name.
UNNAMEABLE_BY_THE_DIRECT_FLOW = {"xml", "point", "short"}


@pytest.mark.parametrize("case", GOLDEN, ids=lambda c: c["postgres"])
def test_types_are_spelled_exactly_as_the_direct_flow_spells_them(case):
    """golden_types.json holds what get_schema_structure really wrote for each PostgreSQL type."""
    if case["postgres"] in UNNAMEABLE_BY_THE_DIRECT_FLOW or case["direct_flow"] in ("NULL", "DOMAIN"):
        assert normalize_type(case["postgres"]) == case["postgres"]  # kept, not turned into NULL
    elif case["postgres"] == "mood":
        pytest.skip("enums are mapped from their labels, see the next test")
    elif case["postgres"] == "mood[]":
        assert normalize_type(case["postgres"]) == "ARRAY"
    else:
        assert normalize_type(case["postgres"]) == case["direct_flow"]


def test_an_enum_column_is_varchar_of_its_longest_label_like_the_direct_flow():
    out = schema_mapper.map_schema(mall_schema())["tables"][0]
    status = next(c for c in out["columns"] if c["name"] == "status")
    assert status == {"name": "status", "type": "VARCHAR(6)", "enum_values": ["open", "closed"]}  # 'closed' = 6


def test_an_array_of_enums_is_just_an_array():
    from vizai_connector_gateway import SchemaColumn, SchemaTable

    table = SchemaTable("t", "public", "table", (SchemaColumn("moods", "mood[]", enum_values=("a", "b")),))
    out = schema_mapper.map_schema(replace(mall_schema(), tables=(table,)))["tables"][0]
    assert out["columns"][0]["type"] == "ARRAY"


def test_a_clashing_table_name_is_qualified_but_a_unique_one_stays_bare():
    payload = mall_schema()
    other = replace(payload.tables[0], schema="archive")
    payload = replace(payload, tables=payload.tables + (other,))
    out = schema_mapper.map_schema(payload)["tables"]
    names = [t["name"] for t in out]
    assert names.count("malls") == 0 and "public.malls" in names and "archive.malls" in names
    qualified = next(t for t in out if t["name"] == "archive.malls")
    assert qualified["table"] == "malls" and qualified["schema"] == "archive"
    assert "stores" in names  # unique: bare, like the direct flow


def test_the_ds_graph_builds_from_the_mapped_schema():
    from app.services.ds_graph import DSGraphBuilder

    graph = DSGraphBuilder(db_type="postgres").build(schema_mapper.map_schema(mall_schema()))
    assert graph["stats"]["table_count"] == 3
    assert graph["stats"]["relation_count"] == 1  # stores.mall_id -> malls
    edge = graph["edges"][0]
    assert (edge["source"], edge["target"], edge["source_column"]) == ("stores", "malls", "mall_id")


class TestValidate:
    def test_a_normal_schema_is_fine(self):
        assert schema_mapper.validate(mall_schema()) is None

    def test_other_dialects_are_refused_for_now(self):
        assert "not supported" in schema_mapper.validate(mall_schema(dialect="mysql"))

    def test_an_empty_schema_is_refused(self):
        assert "no tables" in schema_mapper.validate(replace(mall_schema(), tables=()))

    def test_too_many_tables_are_refused(self, monkeypatch):
        monkeypatch.setattr(schema_mapper, "MAX_TABLES", 2)
        assert "limit" in schema_mapper.validate(mall_schema())

    def test_a_nameless_table_is_refused(self):
        payload = mall_schema()
        broken = replace(payload.tables[0], name="")
        assert "without a name" in schema_mapper.validate(replace(payload, tables=(broken,)))
