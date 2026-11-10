"""Static Terraform grant regression tests for the Phase 3 Silver foundation.

Text assertions only: no Terraform execution, no remote interaction. The
opt-in Phase 3 code in `infra/databricks/main.tf` is asserted on its own text
so the dedicated Silver/quarantine/gold job service principal's least-privilege
boundary cannot drift silently: catalog USE_CATALOG, Bronze USE_SCHEMA plus
read-only SELECT on exactly three raw tables, and USE_SCHEMA plus CREATE_TABLE
on silver/quarantine/gold only. The identity is an existing one that this root
only looks up; no landing, external location, credential, volume, or
pipeline_state access is ever granted, and every grant carries prevent_destroy.
`terraform fmt -check`, `terraform init -backend=false` and `terraform
validate` are operator commands, never part of the unittest suite.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INFRA = ROOT / "infra" / "databricks"
MAIN_TF = (INFRA / "main.tf").read_text(encoding="utf-8")
VARIABLES_TF = (INFRA / "variables.tf").read_text(encoding="utf-8")
TFVARS_EXAMPLE = (INFRA / "terraform.tfvars.example").read_text(encoding="utf-8")

ENABLE_FLAG = "var.enable_phase3_silver_foundation"
APPROVAL_FLAG = "var.phase3_silver_foundation_approved"
RUN_AS_VARIABLE = "var.phase3_silver_job_run_as_application_id"
PHASE3_COUNT_EXPR = f"count = {ENABLE_FLAG} && {APPROVAL_FLAG} ? 1 : 0"

PHASE3_GATE = 'resource "terraform_data" "phase3_silver_foundation_gate"'
PHASE3_LOOKUP = 'data "databricks_service_principal" "phase3_silver_job_run_as"'
PHASE3_CATALOG_GRANT = 'resource "databricks_grant" "phase3_silver_job_catalog"'
PHASE3_BRONZE_SCHEMA_GRANT = 'resource "databricks_grant" "phase3_silver_job_bronze_schema"'
PHASE3_BRONZE_READ_GRANT = 'resource "databricks_grant" "phase3_silver_job_bronze_read"'
PHASE3_TARGET_SCHEMA_GRANT = 'resource "databricks_grant" "phase3_silver_job_target_schema"'

PHASE3_GRANTS = (
    PHASE3_CATALOG_GRANT,
    PHASE3_BRONZE_SCHEMA_GRANT,
    PHASE3_BRONZE_READ_GRANT,
    PHASE3_TARGET_SCHEMA_GRANT,
)
PHASE3_BLOCKS = (PHASE3_GATE, PHASE3_LOOKUP) + PHASE3_GRANTS

BRONZE_READ_LOCAL = "phase3_silver_bronze_read_tables"
TARGET_SCHEMA_PRIV_LOCAL = "phase3_silver_target_schema_privileges"
BRONZE_SELECT_TABLES = ("orders_raw", "customers_raw", "products_raw")
FORBIDDEN_BRONZE_TABLE = "ingestion_run_log"
TARGET_SCHEMAS = ("silver", "quarantine", "gold")

# Phase 2 identifiers the Phase 3 opt-in must never gate on or reference.
PHASE2_IDENTIFIERS = (
    "enable_phase2_ingestion_foundation",
    "phase2_ingestion_foundation_approved",
    "phase2_job_run_as_application_id",
    "phase2_ingestion_foundation_gate",
)

# Securables and privileges the Phase 3 principal must never receive.
FORBIDDEN_IN_PHASE3_GRANTS = (
    "external_location",
    "volume",
    "credential",
    "pipeline_state",
    "landing",
    "READ_FILES",
    "MANAGE",
    "MODIFY",
    "WRITE_FILES",
    "READ_VOLUME",
    "WRITE_VOLUME",
    "CREATE_VOLUME",
    "azure",
)

PHASE2_BRONZE_GRANT = 'resource "databricks_grant" "phase2_job_bronze_schema"'
WRITER_PRIV_LOCAL = "engineer_writer_schema_privileges"


def _normalized(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _block(header: str) -> str:
    index = MAIN_TF.index(header)
    # terraform fmt keeps nested closes indented, so the first closing brace
    # at column 0 after the header ends the block.
    match = re.compile(r"[\s\S]*?\n\}").match(MAIN_TF, index)
    if match is None:
        raise AssertionError(f"could not extract block: {header}")
    return match.group(0)


def _grant_blocks() -> list[tuple[str, str]]:
    """Brace-balanced extraction of every `resource "databricks_grant"` block,
    so a new or renamed grant cannot hide from the exclusivity assertions."""
    blocks: list[tuple[str, str]] = []
    for match in re.finditer(r'resource\s+"databricks_grant"\s+"([^"]+)"\s*\{', MAIN_TF):
        header = re.sub(r"\s+", " ", match.group(0)).strip()
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
            raise AssertionError(f"unbalanced braces in databricks_grant {match.group(1)}")
        blocks.append((header, MAIN_TF[start:cursor - 1]))
    if not blocks:
        raise AssertionError("no databricks_grant blocks found in main.tf")
    return blocks


def _local_map_body(local_name: str) -> str:
    pattern = re.escape(local_name) + r"\s*=\s*\{(?P<body>.*?)\n\s*\}"
    match = re.search(pattern, MAIN_TF, re.DOTALL)
    if match is None:
        raise AssertionError(f"local {local_name} not found in main.tf")
    return match.group("body")


def _map_entry(body: str, key: str) -> str | None:
    match = re.search(rf"\b{re.escape(key)}\s*=\s*(?P<value>.+)", body)
    return match.group("value").strip() if match else None


def _variable_block(name: str) -> str:
    header = f'variable "{name}" {{'
    index = VARIABLES_TF.index(header)
    match = re.compile(r"[\s\S]*?\n\}").match(VARIABLES_TF, index)
    if match is None:
        raise AssertionError(f"could not extract variable block: {name}")
    return match.group(0)


def _normalized_for_each(local_name: str) -> str:
    return f"for_each = {ENABLE_FLAG} && {APPROVAL_FLAG} ? local.{local_name} : {{}}"


class Phase3GateTests(unittest.TestCase):
    def test_gate_is_counted_by_the_enable_flag_only(self):
        block = _normalized(_block(PHASE3_GATE))
        self.assertIn("count = var.enable_phase3_silver_foundation ? 1 : 0", block)
        self.assertNotIn("count = var.enable_phase3_silver_foundation &&", block)

    def test_gate_input_echoes_approval_and_application_id(self):
        block = _normalized(_block(PHASE3_GATE))
        self.assertIn("approved = var.phase3_silver_foundation_approved", block)
        self.assertIn(
            "job_run_as_application_id = var.phase3_silver_job_run_as_application_id",
            block,
        )

    def test_gate_precondition_requires_explicit_approval(self):
        block = _block(PHASE3_GATE)
        self.assertIn("condition     = var.phase3_silver_foundation_approved", block)

    def test_gate_error_states_the_least_privilege_model(self):
        error = _normalized(_block(PHASE3_GATE))
        self.assertIn("least-privilege", error)
        self.assertIn("USE_CATALOG", error)
        self.assertIn("read-only SELECT on exactly the three raw tables", error)
        self.assertIn("USE_SCHEMA plus CREATE_TABLE only on silver, quarantine, and gold", error)
        self.assertIn("no landing, external location, credential, volume, or pipeline_state access", error)

    def test_gate_precondition_requires_a_non_empty_application_id(self):
        error = _normalized(_block(PHASE3_GATE))
        self.assertIn(
            "condition = var.phase3_silver_job_run_as_application_id != null "
            "&& length(trimspace(var.phase3_silver_job_run_as_application_id)) > 0",
            error,
        )
        self.assertIn("existing, dedicated Databricks service principal", error)
        self.assertIn("never creates or manages identities", error)


class Phase3ServicePrincipalLookupTests(unittest.TestCase):
    def test_lookup_is_count_gated_by_both_phase3_flags(self):
        self.assertIn(PHASE3_COUNT_EXPR, _normalized(_block(PHASE3_LOOKUP)))

    def test_lookup_searches_by_the_phase3_application_id(self):
        block = _block(PHASE3_LOOKUP)
        self.assertIn(f"application_id = {RUN_AS_VARIABLE}", block)

    def test_lookup_is_a_data_block_and_no_service_principal_is_created(self):
        self.assertTrue(_block(PHASE3_LOOKUP).lstrip().startswith('data "databricks_service_principal"'))
        self.assertNotIn('resource "databricks_service_principal"', MAIN_TF)


class Phase3GrantGatingTests(unittest.TestCase):
    def test_every_phase3_grant_is_gated_by_both_phase3_flags(self):
        expected_gating = {
            PHASE3_CATALOG_GRANT: PHASE3_COUNT_EXPR,
            PHASE3_BRONZE_SCHEMA_GRANT: PHASE3_COUNT_EXPR,
            PHASE3_BRONZE_READ_GRANT: _normalized_for_each(BRONZE_READ_LOCAL),
            PHASE3_TARGET_SCHEMA_GRANT: _normalized_for_each(TARGET_SCHEMA_PRIV_LOCAL),
        }
        for header, expression in expected_gating.items():
            self.assertIn(expression, _normalized(_block(header)), msg=header)

    def test_no_phase3_block_gates_on_or_references_phase2_identifiers(self):
        for header in PHASE3_BLOCKS:
            block = _normalized(_block(header))
            for phase2_identifier in PHASE2_IDENTIFIERS:
                self.assertNotIn(phase2_identifier, block, msg=header)


class Phase3GrantWiringTests(unittest.TestCase):
    def test_every_phase3_grant_targets_the_dedicated_principal(self):
        for header in PHASE3_GRANTS:
            self.assertIn(f"principal = {RUN_AS_VARIABLE}", _normalized(_block(header)), msg=header)

    def test_every_phase3_grant_prevents_destroy(self):
        for header in PHASE3_GRANTS:
            self.assertIn("prevent_destroy = true", _block(header), msg=header)

    def test_every_phase3_grant_depends_on_the_gate_and_the_lookup(self):
        for header in PHASE3_GRANTS:
            self.assertIn(
                "depends_on = [ terraform_data.phase3_silver_foundation_gate, "
                "data.databricks_service_principal.phase3_silver_job_run_as, ]",
                _normalized(_block(header)),
                msg=header,
            )


class Phase3LeastPrivilegeGrantModelTests(unittest.TestCase):
    def test_catalog_grant_is_use_catalog_only(self):
        block = _block(PHASE3_CATALOG_GRANT)
        self.assertIn("catalog = databricks_catalog.synthetic.name", block)
        self.assertIn('privileges = ["USE_CATALOG"]', block)
        self.assertNotIn("SELECT", block)

    def test_bronze_schema_grant_is_use_schema_only(self):
        block = _block(PHASE3_BRONZE_SCHEMA_GRANT)
        self.assertIn('schema = databricks_schema.phase1["bronze"].id', block)
        self.assertIn('privileges = ["USE_SCHEMA"]', block)
        self.assertNotIn("CREATE_TABLE", block)
        self.assertNotIn("SELECT", block)

    def test_bronze_reads_are_declared_in_a_computed_local_map(self):
        body = _local_map_body(BRONZE_READ_LOCAL)
        keys = re.findall(r'(\w+)\s*=\s*"', body)
        self.assertEqual(sorted(keys), sorted(BRONZE_SELECT_TABLES))
        for table in BRONZE_SELECT_TABLES:
            self.assertEqual(
                _map_entry(body, table),
                f'"${{var.catalog_name}}.bronze.{table}"',
                msg=f"unexpected identifier for {table}",
            )

    def test_bronze_read_grant_is_select_on_the_full_table_name_only(self):
        block = _normalized(_block(PHASE3_BRONZE_READ_GRANT))
        self.assertIn(_normalized_for_each(BRONZE_READ_LOCAL), block)
        self.assertIn("table = each.value", block)
        self.assertIn('privileges = ["SELECT"]', block)

    def test_ingestion_run_log_is_never_granted_to_phase3(self):
        phase3_text = (
            "".join(_block(header) for header in PHASE3_BLOCKS)
            + _local_map_body(BRONZE_READ_LOCAL)
            + _local_map_body(TARGET_SCHEMA_PRIV_LOCAL)
        )
        self.assertNotIn(FORBIDDEN_BRONZE_TABLE, phase3_text)

    def test_target_schemas_get_the_computed_privilege_map(self):
        body = _local_map_body(TARGET_SCHEMA_PRIV_LOCAL)
        keys = re.findall(r"(\w+)\s*=\s*\[", body)
        self.assertEqual(sorted(keys), sorted(TARGET_SCHEMAS))
        for schema in TARGET_SCHEMAS:
            self.assertEqual(
                _map_entry(body, schema),
                '["USE_SCHEMA", "CREATE_TABLE"]',
                msg=f"unexpected privileges for {schema}",
            )

    def test_target_schema_grant_iterates_the_map_without_literal_create_table(self):
        block = _block(PHASE3_TARGET_SCHEMA_GRANT)
        self.assertIn(_normalized_for_each(TARGET_SCHEMA_PRIV_LOCAL), _normalized(block))
        self.assertIn("schema = databricks_schema.phase1[each.key].id", block)
        self.assertIn("privileges = each.value", block)
        self.assertNotIn("CREATE_TABLE", block)

    def test_phase3_grants_never_touch_landing_locations_credentials_volumes_or_state(self):
        for header in PHASE3_GRANTS:
            body = _block(header)
            for forbidden in FORBIDDEN_IN_PHASE3_GRANTS:
                self.assertNotIn(forbidden, body, msg=f"{header} must not reference {forbidden}")


class Phase3FamilyExclusivityTests(unittest.TestCase):
    def test_main_tf_declares_exactly_the_expected_phase3_blocks(self):
        found = [
            re.sub(r"\s+", " ", match.group(0)).strip()
            for match in re.finditer(
                r'(?:resource|data)\s+"[^"]+"\s+"[^"]*phase3[^"]*"\s*\{', MAIN_TF)
        ]
        self.assertEqual(sorted(found), sorted(header + " {" for header in PHASE3_BLOCKS))

    def test_no_grant_outside_the_phase3_family_targets_the_phase3_principal(self):
        consumers = [
            header for header, body in _grant_blocks()
            if RUN_AS_VARIABLE in body
        ]
        self.assertEqual(
            sorted(consumers),
            sorted(header + " {" for header in PHASE3_GRANTS),
            msg="only the four Phase 3 grants may target the Phase 3 principal",
        )


class Phase3VariableDefinitionTests(unittest.TestCase):
    IDENTITY_PHRASES = (
        "existing",
        "separate from the Bronze",
        "looks it up",
        "never creates or manages",
        "Azure RBAC",
    )

    def test_enable_flag_defaults_to_false_and_describes_the_identity_boundary(self):
        block = _variable_block("enable_phase3_silver_foundation")
        self.assertIn("type        = bool", block)
        self.assertIn("default     = false", block)
        for phrase in self.IDENTITY_PHRASES:
            self.assertIn(phrase, block)

    def test_approval_flag_defaults_to_false(self):
        block = _variable_block("phase3_silver_foundation_approved")
        self.assertIn("default     = false", block)

    def test_application_id_is_nullable_with_only_non_empty_validation(self):
        block = _variable_block("phase3_silver_job_run_as_application_id")
        self.assertIn("type        = string", block)
        self.assertIn("default     = null", block)
        self.assertIn(
            "condition     = var.phase3_silver_job_run_as_application_id == null "
            "|| length(trimspace(var.phase3_silver_job_run_as_application_id)) > 0",
            block,
        )
        for phrase in self.IDENTITY_PHRASES:
            self.assertIn(phrase, block)


class Phase3TfvarsExampleTests(unittest.TestCase):
    def test_example_file_lists_all_three_variables_commented_with_inert_values(self):
        self.assertRegex(TFVARS_EXAMPLE, r"#\s*enable_phase3_silver_foundation\s*=\s*false\b")
        self.assertRegex(TFVARS_EXAMPLE, r"#\s*phase3_silver_foundation_approved\s*=\s*false\b")
        self.assertRegex(TFVARS_EXAMPLE, r"#\s*phase3_silver_job_run_as_application_id\s*=\s*null\b")

    def test_example_file_never_sets_phase3_variables_uncommented(self):
        for name in (
            "enable_phase3_silver_foundation",
            "phase3_silver_foundation_approved",
            "phase3_silver_job_run_as_application_id",
        ):
            self.assertIsNone(
                re.search(rf"^\s*{re.escape(name)}\s*=", TFVARS_EXAMPLE, re.MULTILINE),
                msg=f"{name} must stay commented out in terraform.tfvars.example",
            )

    def test_example_file_documents_the_existing_identity_and_no_secrets(self):
        self.assertIn("separate from the Bronze run-as", TFVARS_EXAMPLE)
        self.assertIn("never creates or manages Entra applications", TFVARS_EXAMPLE)
        self.assertIn("secrets, or", TFVARS_EXAMPLE)
        self.assertIn("Azure RBAC", TFVARS_EXAMPLE)


class Phase2ProtectionsRemainIntactTests(unittest.TestCase):
    def test_phase2_run_as_bronze_grant_is_unchanged(self):
        block = _normalized(_block(PHASE2_BRONZE_GRANT))
        self.assertIn(
            "count = var.enable_phase2_ingestion_foundation "
            "&& var.phase2_ingestion_foundation_approved ? 1 : 0",
            block,
        )
        self.assertIn('schema = databricks_schema.phase1["bronze"].id', block)
        self.assertIn('privileges = ["USE_SCHEMA", "CREATE_TABLE"]', block)
        self.assertIn("principal = var.phase2_job_run_as_application_id", block)
        self.assertIn("prevent_destroy = true", block)

    def test_writer_group_keeps_bronze_read_only_and_target_write_foundation(self):
        body = _local_map_body(WRITER_PRIV_LOCAL)
        self.assertIsNotNone(re.search(r'\bbronze\s*=\s*\["USE_SCHEMA"\]', body))
        for schema in TARGET_SCHEMAS:
            self.assertIsNotNone(
                re.search(rf'\b{schema}\s*=\s*\["USE_SCHEMA", "CREATE_TABLE"\]', body),
                msg=f"writer group lost CREATE_TABLE on {schema}",
            )


if __name__ == "__main__":
    unittest.main()
