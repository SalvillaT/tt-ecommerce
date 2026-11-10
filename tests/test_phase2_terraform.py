"""Static Terraform grant regression tests for the Phase 2 UC permission fix.

Text assertions only: no Terraform execution, no remote interaction. The
`infra/databricks/main.tf` HCL is asserted on its own text so the Bronze
write-ownership split cannot drift silently: the `engineer_writer` group keeps
`CREATE_TABLE` on the silver/quarantine/gold Phase 1 schemas but must never get
it on bronze, while the Phase 2 run-as service principal keeps `CREATE_TABLE`
on bronze. `terraform fmt -check`, `terraform init -backend=false` and
`terraform validate` are operator commands, never part of the unittest suite.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN_TF = (ROOT / "infra" / "databricks" / "main.tf").read_text(encoding="utf-8")

WRITER_PRIV_LOCAL = "engineer_writer_schema_privileges"
WRITER_GRANT = 'resource "databricks_grant" "engineer_writer_schema"'
RUN_AS_BRONZE_GRANT = 'resource "databricks_grant" "phase2_job_bronze_schema"'
RUN_AS_PRINCIPAL = "var.phase2_job_run_as_application_id"
WRITER_PRINCIPAL = "var.engineer_writer_group_name"
USE_SCHEMA_PLUS_CREATE_TABLE = '["USE_SCHEMA", "CREATE_TABLE"]'

CREATE_TABLE_SCHEMAS = ("silver", "quarantine", "gold")


def _local_priv_map_body() -> str:
    match = re.search(
        rf"{WRITER_PRIV_LOCAL}\s*=\s*\{{(?P<body>.*?)\n\s*\}}",
        MAIN_TF,
        re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"missing/{WRITER_PRIV_LOCAL} local not found in main.tf")
    return match.group("body")


def _resource_block(resource_header: str) -> str:
    index = MAIN_TF.index(resource_header)
    # terraform fmt keeps nested closes indented, so the first closing brace
    # at column 0 after the header ends the resource block.
    match = re.compile(r"[\s\S]*?\n\}").match(MAIN_TF, index)
    if match is None:
        raise AssertionError(f"could not extract block: {resource_header}")
    return match.group(0)


def _grant_blocks() -> list[tuple[str, str]]:
    """Pure-stdlib extraction of every `resource "databricks_grant"` block.

    Returns `(header, body)` pairs with brace-balanced bodies (no third-party
    HCL parser): the scan walks each `resource "databricks_grant" ... {` and
    closes at the matching `}`. Unknown/renamed grants must therefore surface
    in this list too, which is what the exclusivity assertions below need:
    a NEW declarative grant cannot hide from the hole-patching tests.
    """
    blocks: list[tuple[str, str]] = []
    for match in re.finditer(r'resource\s+"databricks_grant"\s+"([^"]+)"\s*\{', MAIN_TF):
        header = re.sub(r"\s+", " ", match.group(0)).strip()
        name = match.group(1)
        start = match.end()
        depth = 1
        cursor = start
        while depth and cursor < len(MAIN_TF):
            char = MAIN_TF[cursor]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
            cursor += 1
        if depth:
            raise AssertionError(f"unbalanced braces in databricks_grant {name}")
        blocks.append((header, MAIN_TF[start:cursor - 1]))
    if not blocks:
        raise AssertionError("no databricks_grant blocks found in main.tf")
    return blocks


class EngineerWriterSchemaGrantTests(unittest.TestCase):
    def _body(self) -> str:
        return _local_priv_map_body()

    def test_writer_keeps_create_table_on_phase1_writer_schemas(self):
        body = self._body()
        for schema in CREATE_TABLE_SCHEMAS:
            self.assertIsNotNone(
                re.search(
                    rf'\b{schema}\s*=\s*\["USE_SCHEMA", "CREATE_TABLE"\]', body
                ),
                msg=f"engineer_writer lost CREATE_TABLE on {schema}",
            )

    def test_writer_has_no_create_table_on_bronze(self):
        body = self._body()
        match = re.search(r"\bbronze\s*=\s*\[(?P<privs>[^\]]*)\]", body)
        self.assertIsNotNone(match, msg="bronze entry missing from grant local")
        bronze_privileges = [p.strip().strip('"') for p in match.group("privs").split(",")]
        self.assertEqual(bronze_privileges, ["USE_SCHEMA"])
        self.assertNotIn("CREATE_TABLE", bronze_privileges)


class WriterGrantResourceWiringTests(unittest.TestCase):
    def test_writer_grant_iterates_the_computed_privilege_map(self):
        block = _resource_block(WRITER_GRANT)
        self.assertIn(f"for_each = local.{WRITER_PRIV_LOCAL}", block)
        self.assertIn("schema = databricks_schema.phase1[each.key].id", block)
        self.assertIn("privileges = each.value", block)
        self.assertIn(WRITER_PRINCIPAL, block)

    def test_writer_grant_resource_has_no_hardcoded_create_table(self):
        block = _resource_block(WRITER_GRANT)
        self.assertNotIn("CREATE_TABLE", block)


class Phase2RunAsBronzeGrantTests(unittest.TestCase):
    def test_run_as_principal_keeps_create_table_on_bronze(self):
        block = _resource_block(RUN_AS_BRONZE_GRANT)
        self.assertIn('schema = databricks_schema.phase1["bronze"].id', block)
        self.assertIn(RUN_AS_PRINCIPAL, block)
        self.assertIn(USE_SCHEMA_PLUS_CREATE_TABLE, block)


class BronzeCreateTableGrantExclusivityTests(unittest.TestCase):
    """Hole-closing regression: scan EVERY declarative grant in main.tf, not
    just the known blocks. Only the Phase 2 run-as service principal may hold
    CREATE_TABLE on bronze, and no other grant block (present or accidentally
    re-added later) may declare a literal CREATE_TABLE outside the writer
    group's computed local map (silver/quarantine/gold). The opt-in Phase 3
    Silver foundation adds exactly one additional, read-only grant on the
    bronze schema; it must never carry CREATE_TABLE."""

    def test_the_run_as_grant_is_the_only_bronze_schema_grant_with_create_table(self):
        bronze_schemas = [
            (header, body) for header, body in _grant_blocks()
            if re.search(
                r'schema\s*=\s*databricks_schema\.phase1\["bronze"\]\.id', body)
        ]
        run_as_blocks = [
            block for block in bronze_schemas
            if block[0] == f'{RUN_AS_BRONZE_GRANT} {{'
        ]
        self.assertEqual(len(run_as_blocks), 1,
                         msg="the Phase 2 run-as bronze grant must appear exactly once")
        _, body = run_as_blocks[0]
        self.assertIn(RUN_AS_PRINCIPAL, body)
        self.assertIn(USE_SCHEMA_PLUS_CREATE_TABLE, body)
        # Every other bronze schema grant (the Phase 3 Silver foundation one,
        # present or re-added later) is read-only: no CREATE_TABLE on bronze.
        for header, other_body in bronze_schemas:
            if header != f'{RUN_AS_BRONZE_GRANT} {{':
                self.assertNotIn(
                    "CREATE_TABLE", other_body,
                    msg=f"grant {header!r} declares CREATE_TABLE on bronze; bronze "
                        f"CREATE_TABLE is reserved for {RUN_AS_BRONZE_GRANT}")

    def test_the_run_as_grant_is_the_only_literal_create_table_grant(self):
        # The engineer_writer CREATE_TABLE lives ONLY in the computed local
        # map (`engineer_writer_schema_privileges`), consumed through
        # `privileges = each.value`; its grant block never spells
        # CREATE_TABLE literally. Any grant block with a literal CREATE_TABLE
        # that is not the Phase 2 run-as bronze grant fails here — including
        # one accidentally granted at catalog level or to the writer group.
        for header, body in _grant_blocks():
            if "CREATE_TABLE" in body:
                self.assertEqual(
                    header, f'{RUN_AS_BRONZE_GRANT} {{',
                    msg=f"grant {header!r} declares CREATE_TABLE; bronze CREATE_TABLE is "
                        f"reserved for the Phase 2 run-as grant ({RUN_AS_BRONZE_GRANT}) and "
                        "the writer group keeps it only via the computed "
                        f"{WRITER_PRIV_LOCAL} map")

    def test_writer_privilege_map_is_consumed_by_exactly_one_grant(self):
        consumers = [header for header, body in _grant_blocks()
                     if WRITER_PRIV_LOCAL in body]
        self.assertEqual(consumers, [f"{WRITER_GRANT} {{"])

    def test_run_as_grant_requires_the_phase2_opt_in_flags(self):
        block = _resource_block(RUN_AS_BRONZE_GRANT)
        self.assertIn("count = var.enable_phase2_ingestion_foundation "
                      "&& var.phase2_ingestion_foundation_approved ? 1 : 0", block)
        self.assertIn("prevent_destroy = true", block)


if __name__ == "__main__":
    unittest.main()
