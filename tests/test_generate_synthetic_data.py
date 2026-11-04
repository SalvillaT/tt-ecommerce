"""Tests Fase 0 del generador de datos sintéticos (solo stdlib unittest).

Ejecutar:  py -m unittest discover -s tests -v
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import unittest
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "generate_synthetic_data.py"
FIXTURE_DIR = ROOT / "data" / "sample" / "DEMO-001"
SEED = 42
BATCH = "DEMO-001"

_spec = importlib.util.spec_from_file_location("generate_synthetic_data", SCRIPT)
gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen)

# Fechas canónicas de celda (espejo de B_A/B_B del generador).
B_A = gen.B_A
B_B = gen.B_B

D2 = Decimal("0.01")


def _money(cents: int) -> Decimal:
    """Entero de céntimos -> Decimal(18,2) exacto."""
    return (Decimal(cents) / Decimal(100)).quantize(D2, rounding=ROUND_HALF_UP)


def _regen(tmp_root: str, seed: int = SEED, batch: str = BATCH) -> dict[str, bytes]:
    return gen.generate_payload(seed, batch)


def _orders_records(payload: dict[str, bytes]) -> list[dict]:
    """Registros parseables del JSONL (ignora la línea malformada intencional)."""
    text = payload[f"orders_{BATCH}.jsonl"].decode("utf-8")
    records: list[dict] = []
    for line in text.split("\n")[:-1]:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records

# ---------------------------------------------------------------------------
# Resolución estilo Silver: estados/importes/referencias y snapshot vigente.
# ---------------------------------------------------------------------------

ORDER_FIELDS = {
    "order_id", "line_id", "record_version", "event_at", "business_date",
    "customer_id", "product_id", "status", "quantity",
    "unit_price_eur", "gross_amount_eur", "refund_amount_eur",
    "net_revenue_eur", "currency", "channel", "region_code", "note",
}

VALID_STATUSES = {"PENDING", "COMPLETED", "CANCELLED", "PARTIALLY_REFUNDED", "REFUNDED"}
AMOUNT_RE = r"^\d+\.\d{2}$"
TS_RE = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
DATE_RE = r"^\d{4}-\d{2}-\d{2}$"


def rescued_fields(rec: dict) -> set:
    """Campos JSON desconocidos del contrato (SCH-001, rescued data, aviso)."""
    return set(rec) - ORDER_FIELDS


def contract_errors(rec: dict) -> list[str]:
    """Bloquea las reglas críticas de contrato v1 (SCH-001 no es crítica)."""
    errors: list[str] = []
    for key in ORDER_FIELDS - {"note"}:
        value = rec.get(key)
        if value is None or value == "":
            errors.append(f"ORD-001 nulo obligatorio: {key}")
    if errors:
        return errors
    import re as _re

    if not _re.fullmatch(TS_RE, rec["event_at"]):
        errors.append("ORD-001 event_at no RFC3339 Z")
    if not _re.fullmatch(DATE_RE, rec["business_date"]):
        errors.append("ORD-001 business_date no ISO")
    if not isinstance(rec["record_version"], int) or rec["record_version"] < 1:
        errors.append("ORD-002 record_version no entero >= 1")
    if not isinstance(rec["quantity"], int) or rec["quantity"] <= 0:
        errors.append("ORD-004 quantity <= 0")
    if rec["status"] not in VALID_STATUSES:
        errors.append("ORD-003 estado fuera de enum")
    if rec["currency"] != "EUR":
        errors.append("ORD-003 moneda distinta de EUR")
    if rec["channel"] not in {"web", "marketplace"}:
        errors.append("ORD-003 canal no permitido")
    if len(rec["note"] or "") > 200:
        errors.append("ORD-001 nota demasiado larga")
    amounts = {k: rec[k] for k in ("unit_price_eur", "gross_amount_eur",
                                   "refund_amount_eur", "net_revenue_eur")}
    for key, text in amounts.items():
        if not _re.fullmatch(AMOUNT_RE, text):
            errors.append(f"ORD-003 importe no decimal(18,2): {key}")
    if errors:
        return errors
    unit = Decimal(rec["unit_price_eur"])
    gross = Decimal(rec["gross_amount_eur"])
    refund = Decimal(rec["refund_amount_eur"])
    net = Decimal(rec["net_revenue_eur"])  # noqa: F841
    if unit > Decimal("1000000") or gross > Decimal("1000000"):
        errors.append("ORD-004 importe fuera de rango 0..1,000,000")
    status = rec["status"]
    # Igualdad aritmética de contrato en TODOS los estados:
    # gross = quantity x unit_price siempre; para PENDING/CANCELLED además
    # refund = net = 0 (el bruto es valor potencial, no venta realizada);
    # para COMPLETED/PARTIALLY_REFUNDED/REFUNDED además net = gross - refund.
    if unit * rec["quantity"] != gross:
        errors.append("ORD-003 gross != quantity x unit_price")
    if status in ("PENDING", "CANCELLED"):
        if refund != 0 or net != 0:
            errors.append("ORD-003 PENDING/CANCELLED exige refund = net = 0")
    else:
        if net != gross - refund:
            errors.append("ORD-003 net != gross - refund")
        if status == "COMPLETED" and refund != 0:
            errors.append("ORD-003 COMPLETED con refund > 0")
        if status == "REFUNDED" and refund != gross:
            errors.append("ORD-003 REFUNDED exige refund = gross")
        if status == "PARTIALLY_REFUNDED" and not (0 < refund < gross):
            errors.append("ORD-003 PARTIAL exige 0 < refund < gross")
    return errors


def resolve_snapshot(payload: dict[str, bytes]) -> dict:
    """Resuelve la instantánea vigente estilo Silver (v1):
    1. descarta líneas no parseables (SCH-002);
    2. verifica contrato por registro (ORD-001/003/004);
    3. empates de clave+versión con payload distinto -> conflicto (ORD-002),
       payload exacto repetido -> se ignora el duplicado (ORD-006);
    4. fila vigente = mayor record_version por clave;
    5. referencias a maestros vigentes válidos (ORD-005 / CUS-001).
    """
    text = payload[f"orders_{BATCH}.jsonl"].decode("utf-8")
    lines = text.split("\n")[:-1]
    malformed: list[str] = []
    records: list[dict] = []
    for line in lines:
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            malformed.append(line)
            continue
        records.append(rec)

    invalid = [r for r in records if contract_errors(r)]
    valid = [r for r in records if r not in invalid]

    from collections import defaultdict
    by_key_version: dict[tuple, list[dict]] = defaultdict(list)
    for rec in valid:
        key = (rec["order_id"], rec["line_id"], rec["record_version"])
        by_key_version[key].append(rec)

    conflicted_keys: list[tuple] = []
    unique: list[dict] = []
    for key, rows in by_key_version.items():
        canonical = {json.dumps(r, sort_keys=True, ensure_ascii=False) for r in rows}
        if len(canonical) > 1:
            conflicted_keys.append(key)
        else:
            unique.append(rows[0])
    # La línea entera queda en cuarentena: ninguna versión de la clave produce
    # fila Silver vigente hasta resolver el conflicto (ORD-002).
    conflicted_pairs = {(k[0], k[1]) for k in conflicted_keys}

    customers = _load_master(payload[f"customers_{BATCH}.csv"])
    products = _load_master(payload[f"products_{BATCH}.csv"])
    # Validación de maestros: clientes requieren email sintético no vacío y
    # versión vigente (CUS-001); productos categoría/moneda/precio/active
    # (PRD-001). Los inválidos quedan cuarentenados en Silver.
    import re as _re
    email_re = _re.compile(r"^[^@\s]+@[^@\s]+\.[a-z]+$")
    valid_customers = {
        cid for cid, row in customers.items()
        if bool(row["email"]) and email_re.fullmatch(row["email"])
        and int(row["record_version"]) == 1 and row["region_code"]
    }
    invalid_customers = set(customers) - valid_customers
    valid_products = {
        pid for pid, row in products.items()
        if row["active"] == "true" and int(row["record_version"]) == 1
        and row["category"] in ("paddle", "rubber", "ball", "table", "net",
                                "accessory")
        and Decimal(row["list_price_eur"]) >= 0 and row["currency"] == "EUR"
    }
    invalid_products = set(products) - valid_products

    latest: dict[tuple, dict] = {}
    orphan: list[dict] = []
    for rec in unique:
        if (rec["order_id"], rec["line_id"]) in conflicted_pairs:
            continue
        key = (rec["order_id"], rec["line_id"])
        current = latest.get(key)
        if current is None or rec["record_version"] > current["record_version"]:
            latest[key] = rec
    snapshot: list[dict] = []
    for rec in latest.values():
        if rec["customer_id"] not in valid_customers or rec["product_id"] not in valid_products:
            orphan.append(rec)
            continue
        snapshot.append(rec)

    return {
        "malformed": malformed,
        "invalid": invalid,
        "conflicted_keys": conflicted_keys,
        "orphan": orphan,
        "snapshot": snapshot,
        "invalid_customers": invalid_customers,
        "invalid_products": invalid_products,
    }


def _load_master(csv_bytes: bytes) -> dict[str, dict[str, str]]:
    reader = csv.DictReader(io.StringIO(csv_bytes.decode("utf-8")))
    key = "customer_id" if "customer_id" in reader.fieldnames else "product_id"
    return {row[key]: row for row in reader}


def gold_metrics(snapshot: list[dict]) -> dict:
    """Métricas Gold: revenue Decimal (COMPLETED/PARTIALLY), count distinct
    de order_id elegibles, y líneas reembolsadas. Sin floats."""
    rev = Decimal("0")
    orders: set[str] = set()
    refunded = 0
    for rec in snapshot:
        status = rec["status"]
        if status in ("COMPLETED", "PARTIALLY_REFUNDED"):
            rev += Decimal(rec["net_revenue_eur"])
            if Decimal(rec["net_revenue_eur"]) > 0:
                orders.add(rec["order_id"])
        if status in ("REFUNDED", "PARTIALLY_REFUNDED"):
            refunded += 1
    return {"total_revenue_eur": rev, "order_count": len(orders),
            "refunded_line_count": refunded}


# ---------------------------------------------------------------------------
# Gating Gold PUB-001: celda canónica (business_date, category, region_code,
# channel); privacy_subject_count = customer_id distintos de líneas vigentes
# válidas COMPLETED/PARTIALLY_REFUNDED/REFUNDED; k=5; celda < k se suprime
# íntegra (ninguna medida, sin rollups).
# ---------------------------------------------------------------------------

K_THRESHOLD = 5
GATING_STATUSES = {"COMPLETED", "PARTIALLY_REFUNDED", "REFUNDED"}


def gate_cells(info: dict, payload: dict[str, bytes]) -> tuple[dict, dict]:
    """Agrupa el snapshot validado por celda canónica usando la categoría del
    maestro de productos; separa celdas publicables vs suprimidas por sujetos."""
    products = _load_master(payload[f"products_{BATCH}.csv"])
    cells: dict[tuple, list[dict]] = {}
    for rec in info["snapshot"]:
        category = products[rec["product_id"]]["category"]
        cell = (rec["business_date"], category, rec["region_code"], rec["channel"])
        cells.setdefault(cell, []).append(rec)
    published: dict[tuple, dict] = {}
    suppressed: dict[tuple, dict] = {}
    for cell, rows in cells.items():
        subjects = {r["customer_id"] for r in rows if r["status"] in GATING_STATUSES}
        entry = {"subjects": subjects, "rows": rows}
        if len(subjects) >= K_THRESHOLD:
            published[cell] = entry
        else:
            suppressed[cell] = entry
    return published, suppressed


def published_measures(published: dict) -> dict:
    """Las 3 medidas Gold sobre las celdas publicables, sumando count-distinct
    dentro de cada celda (sin rollups desde celdas suprimidas)."""
    revenue = Decimal("0")
    orders: set[str] = set()
    refunded_lines = 0
    for entry in published.values():
        cell_orders: set[str] = set()
        for rec in entry["rows"]:
            if rec["status"] in ("COMPLETED", "PARTIALLY_REFUNDED"):
                if Decimal(rec["net_revenue_eur"]) > 0:
                    cell_orders.add(rec["order_id"])
                    revenue += Decimal(rec["net_revenue_eur"])
            if rec["status"] in ("REFUNDED", "PARTIALLY_REFUNDED"):
                refunded_lines += 1
        orders |= cell_orders
    return {"total_revenue_eur": revenue, "order_count": len(orders),
            "refunded_line_count": refunded_lines}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class Reproducibility(unittest.TestCase):
    """Contrato: mismas entradas -> mismos bytes, incluso en directorios distintos."""

    def test_two_runs_same_seed_batch_identical_bytes(self):
        with tempfile.TemporaryDirectory() as t1, tempfile.TemporaryDirectory() as t2:
            gen.write_payload(_regen(t1), t1)
            gen.write_payload(_regen(t2), t2)
            names = [f"orders_{BATCH}.jsonl", f"customers_{BATCH}.csv",
                     f"products_{BATCH}.csv", f"manifest_{BATCH}.json"]
            for name in names:
                b1 = (Path(t1) / BATCH / name).read_bytes()
                b2 = (Path(t2) / BATCH / name).read_bytes()
                self.assertEqual(b1, b2, f"{name} difiere entre runs")

    def test_output_is_output_batchid_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = gen.write_payload(_regen(tmp), tmp)
            self.assertEqual(out, Path(tmp) / BATCH)
            self.assertEqual(
                sorted(p.name for p in out.iterdir()),
                sorted([f"orders_{BATCH}.jsonl", f"customers_{BATCH}.csv",
                        f"products_{BATCH}.csv", f"manifest_{BATCH}.json"]),
            )

    def test_seed_changes_data_materially(self):
        a = _regen("x", SEED)
        b = _regen("x", SEED + 1)
        self.assertNotEqual(a, b)
        orders_a = a[f"orders_{BATCH}.jsonl"]
        orders_b = b[f"orders_{BATCH}.jsonl"]
        self.assertNotEqual(orders_a, orders_b)
        recs_a = _orders_records(a)
        recs_b = _orders_records(b)
        baseline_a = {r["order_id"]: r for r in recs_a if r["order_id"].startswith("ORD-BASE-")}
        baseline_b = {r["order_id"]: r for r in recs_b if r["order_id"].startswith("ORD-BASE-")}
        diffs = [oid for oid in baseline_a
                 if (baseline_a[oid]["quantity"], baseline_a[oid]["product_id"])
                 != (baseline_b[oid]["quantity"], baseline_b[oid]["product_id"])]
        self.assertTrue(len(diffs) >= 1, "la seed debe alterar el baseline")
        # No debe tocar los escenarios fijos identificados por ID.
        fixed_ids = ["ORD-DUP-01", "ORD-CORR-01", "ORD-CANC-01", "ORD-REF-01",
                     "ORD-REF-02", "ORD-LATE-01", "ORD-ORPH-01", "ORD-EXTRA-01",
                     "ORD-NULL-01", "ORD-CONF-01"]
        fixed_a = [r for r in recs_a if r["order_id"] in fixed_ids]
        fixed_b = [r for r in recs_b if r["order_id"] in fixed_ids]
        self.assertEqual(fixed_a, fixed_b)


class EncodingAndFormat(unittest.TestCase):
    """Codificación UTF-8 sin BOM, LF, CSV RFC 4180 con cabecera, JSONL 1/linea."""

    @classmethod
    def setUpClass(cls):
        cls.payload = _regen("fmt", SEED)

    def test_no_bom_lf_utf8_everywhere(self):
        for name, data in self.payload.items():
            self.assertFalse(data.startswith(b"\xef\xbb\xbf"), name)
            self.assertNotIn(b"\r", data, f"{name} contiene CR")
            data.decode("utf-8")  # decodable

    def test_jsonl_one_record_per_line(self):
        text = self.payload[f"orders_{BATCH}.jsonl"].decode("utf-8")
        lines = text.split("\n")[:-1]
        self.assertTrue(text.endswith("\n"))
        parseable = 0
        for line in lines:
            try:
                json.loads(line)
                parseable += 1
            except json.JSONDecodeError:
                pass
        self.assertEqual(len(lines), 26)
        self.assertEqual(parseable, 25)  # la 26ª es malformada intencional

    def test_csv_headers_and_parse(self):
        for name, header in ((f"customers_{BATCH}.csv", gen.CUSTOMER_HEADER),
                             (f"products_{BATCH}.csv", gen.PRODUCT_HEADER)):
            text = self.payload[name].decode("utf-8")
            self.assertEqual(text.split("\n")[0], ",".join(header))
            rows = list(csv.reader(io.StringIO(text)))
            self.assertTrue(all(len(row) == len(header) for row in rows))
            self.assertEqual(len(rows) - 1, 11 if "customers" in name else 7)


class ManifestContract(unittest.TestCase):
    """manifest v1: campos exigidos, SHA-256, filas, sin reloj ni rutas."""

    @classmethod
    def setUpClass(cls):
        cls.payload = _regen("man", SEED)
        cls.manifest = json.loads(cls.payload[f"manifest_{BATCH}.json"].decode("utf-8"))

    def test_required_fields(self):
        self.assertEqual(self.manifest["contract_version"], "v1")
        self.assertEqual(self.manifest["generator_version"], "1.0.0")
        self.assertEqual(self.manifest["batch_id"], BATCH)
        self.assertEqual(self.manifest["seed"], SEED)
        scenarios = {s["scenario"] for s in self.manifest["scenarios"]}
        self.assertEqual(scenarios, {
            "baseline_valid", "exact_duplicate", "later_correction", "cancellation",
            "partial_refund", "full_refund", "late_event", "allowed_null",
            "rejected_null", "orphan_reference", "extra_json_field",
            "malformed_json", "version_conflict",
        })

    def test_file_hashes_and_rows(self):
        for entry in self.manifest["files"]:
            name = entry["file"]
            self.assertNotIn("/", name)
            self.assertNotIn("\\", name)
            data = self.payload[name]
            self.assertEqual(entry["sha256"], hashlib.sha256(data).hexdigest())
            text = data.decode("utf-8")
            if name.endswith(".csv"):
                self.assertEqual(entry["rows"], len(text.splitlines()) - 1)
            else:
                self.assertEqual(entry["rows"], len(text.split("\n")) - 1)
        self.assertEqual(self.manifest["total_rows"], 44)

    def test_no_clock_no_abs_paths(self):
        import re
        raw = self.payload[f"manifest_{BATCH}.json"].decode("utf-8")
        self.assertNotIn("data/generated", raw)
        self.assertNotRegex(raw, r"[A-Za-z]:\\")
        # El manifest no lleva timestamps de reloj en ningún campo.
        self.assertIsNone(re.search(r"\d{4}-\d{2}-\d{2}T\d{2}", raw))


class OrdersContractValidation(unittest.TestCase):
    """Esquema y reglas v1 sobre registros parseables; transiciones coherentes."""

    @classmethod
    def setUpClass(cls):
        cls.payload = _regen("ord", SEED)
        cls.recs = _orders_records(cls.payload)
        cls.snapshot_info = resolve_snapshot(cls.payload)

    def test_all_parseable_records_pass_contract_except_design_negatives(self):
        for rec in self.recs:
            errors = contract_errors(rec)
            # Único registro inválido de contrato por diseño: rejected_null
            # (customer_id nulo, ORD-001). Todos los demás deben estar limpios.
            if rec["order_id"] == "ORD-NULL-01":
                self.assertTrue(any("ORD-001" in e for e in errors))
            else:
                self.assertEqual(errors, [], f"{rec['order_id']}")

    def test_malformed_line_isolated_and_detected(self):
        info = self.snapshot_info
        self.assertEqual(len(info["malformed"]), 1)
        malformed = info["malformed"][0]
        self.assertIn("ORD-MALF-01", malformed)
        raw = self.payload[f"orders_{BATCH}.jsonl"].decode("utf-8")
        lines = raw.split("\n")[:-1]
        self.assertEqual(lines[-1], malformed, "malformada debe ser la última línea")
        # y no contamina: la línea anterior es un registro válido y completo
        self.assertEqual(contract_errors(json.loads(lines[-2])), [])

    def test_transitions_are_coherent(self):
        import re
        by_key: dict[tuple, list[dict]] = {}
        seen: set[str] = set()
        for r in self.recs:  # colapsa duplicados byte-equivalentes (ORD-006)
            canonical = json.dumps(r, sort_keys=True, ensure_ascii=False)
            if canonical in seen:
                continue
            seen.add(canonical)
            by_key.setdefault((r["order_id"], r["line_id"]), []).append(r)
        transitions_ok = {
            ("PENDING", "COMPLETED"), ("PENDING", "CANCELLED"),
            ("COMPLETED", "PARTIALLY_REFUNDED"), ("COMPLETED", "REFUNDED"),
            ("PARTIALLY_REFUNDED", "REFUNDED"),
            ("COMPLETED", "COMPLETED"), ("PARTIALLY_REFUNDED", "PARTIALLY_REFUNDED"),
        }
        for key, rows in by_key.items():
            rows.sort(key=lambda r: r["record_version"])
            versions = [r["record_version"] for r in rows]
            if len(versions) != len(set(versions)):
                # Única anomalía admitida: conflicto de clave+versión (ORD-002),
                # registrado explícitamente en el manifest como version_conflict.
                self.assertEqual(key, ("ORD-CONF-01", "1"))
                continue
            self.assertEqual(versions, sorted(set(versions)), f"{key}")
            statuses = [r["status"] for r in rows]
            if len(rows) > 1:
                self.assertIn((statuses[0], statuses[-1]), transitions_ok, f"{key}")

    def test_exact_duplicate_scenario(self):
        raw = self.payload[f"orders_{BATCH}.jsonl"].decode("utf-8")
        dups = [l for l in raw.split("\n") if '"ORD-DUP-01"' in l]
        self.assertEqual(len(dups), 2)
        self.assertEqual(dups[0], dups[1], "los duplicados deben ser byte-equivalentes")
        # En la instantánea una sola versión vigente (ORD-006).
        snap = [r for r in self.snapshot_info["snapshot"] if r["order_id"] == "ORD-DUP-01"]
        self.assertEqual(len(snap), 1)

    def test_later_correction_replaces_v1(self):
        snap = [r for r in self.snapshot_info["snapshot"] if r["order_id"] == "ORD-CORR-01"]
        self.assertEqual(len(snap), 1)
        self.assertEqual(snap[0]["record_version"], 2)
        self.assertEqual(snap[0]["status"], "COMPLETED")
        self.assertEqual(Decimal(snap[0]["net_revenue_eur"]), _money(3600))

    def test_cancellation_zero(self):
        for rec in self.recs:
            if rec["order_id"] == "ORD-CANC-01":
                self.assertEqual(Decimal(rec["refund_amount_eur"]), Decimal("0.00"))
                self.assertEqual(Decimal(rec["net_revenue_eur"]), Decimal("0.00"))
        snap = [r for r in self.snapshot_info["snapshot"] if r["order_id"] == "ORD-CANC-01"]
        self.assertEqual(len(snap), 1)
        self.assertEqual(snap[0]["status"], "CANCELLED")

    def test_partial_refund_coherence(self):
        rec_v2 = next(r for r in self.recs
                      if r["order_id"] == "ORD-REF-01" and r["record_version"] == 2)
        gross, refund, net = (Decimal(rec_v2["gross_amount_eur"]),
                              Decimal(rec_v2["refund_amount_eur"]),
                              Decimal(rec_v2["net_revenue_eur"]))
        self.assertEqual(gross, _money(5500))
        self.assertEqual(net, gross - refund)
        self.assertTrue(Decimal(0) < refund < gross)

    def test_full_refund_zero_net(self):
        rec_v2 = next(r for r in self.recs
                      if r["order_id"] == "ORD-REF-02" and r["record_version"] == 2)
        self.assertEqual(rec_v2["status"], "REFUNDED")
        self.assertEqual(Decimal(rec_v2["net_revenue_eur"]), Decimal("0.00"))
        self.assertEqual(Decimal(rec_v2["refund_amount_eur"]),
                         Decimal(rec_v2["gross_amount_eur"]))

    def test_late_event_fixed_date(self):
        rec = next(r for r in self.recs if r["order_id"] == "ORD-LATE-01")
        self.assertEqual(rec["business_date"], "2026-01-05")
        self.assertEqual(rec["event_at"], "2026-01-10T17:00:00Z")  # fecha fija, no reloj
        self.assertEqual(contract_errors(rec), [])

    def test_allowed_null_phone_accepted(self):
        rows = _load_master(self.payload[f"customers_{BATCH}.csv"])
        self.assertEqual(rows["CUS-SOS-NOPHONE"]["phone"], "")
        self.assertTrue(rows["CUS-SOS-NOPHONE"]["email"])  # email presente

    def test_synthetic_phone_marker_not_assignable(self):
        """Verifica que todo phone no vacío usa el marcador deliberadamente no
        asignable ``^\+340\d{8}$`` (prefijo +34 con 0 inicial del abonado, no
        válido como número real) y que no hay teléfonos reales ``+346...``."""
        import re
        # En ficheros commiteados del fixture:
        for line in FIXTURE_DIR.joinpath(f"customers_{BATCH}.csv").read_text(
                "utf-8").splitlines()[1:]:
            row = line.split(",")
            phone = row[5]
            if phone:
                self.assertRegex(phone, r"^\+340\d{8}$",
                                 f"teléfono no marcador: {phone}")
                # longitud exacta: +34 + 0 + 8 dígitos = número de 9 de abonado
                self.assertEqual(len(phone), 12, phone)
        # y en regeneraciones arbitrarias (no solo fixture commiteado)
        rows = _load_master(self.payload[f"customers_{BATCH}.csv"])
        for row in rows.values():
            phone = row["phone"]
            if phone:
                self.assertRegex(phone, r"^\+340\d{8}$", phone)
        # ningún teléfono real en los bytes del CSV
        raw = self.payload[f"customers_{BATCH}.csv"].decode("utf-8")
        self.assertNotRegex(raw, r"\+346")

    def test_rejected_null_quarantined(self):
        info = self.snapshot_info
        nulls = [r for r in info["invalid"] if r["order_id"] == "ORD-NULL-01"]
        self.assertEqual(len(nulls), 1)
        snapshot_orders = {r["order_id"] for r in info["snapshot"]}
        self.assertNotIn("ORD-NULL-01", snapshot_orders)
        masters = _load_master(self.payload[f"customers_{BATCH}.csv"])
        self.assertEqual(masters["CUS-NULL-EMAIL"]["email"], "")

    def test_orphan_reference(self):
        info = self.snapshot_info
        orphans = [r for r in info["orphan"] if r["order_id"] == "ORD-ORPH-01"]
        self.assertEqual(len(orphans), 1)
        masters = _load_master(self.payload[f"customers_{BATCH}.csv"])
        self.assertNotIn("CUS-GHOST", masters)
        self.assertNotIn("ORD-ORPH-01", {r["order_id"] for r in info["snapshot"]})

    def test_extra_json_field_rescued(self):
        rec = next(r for r in self.recs if r["order_id"] == "ORD-EXTRA-01")
        self.assertIn("source_campaign", rec)
        self.assertEqual(rescued_fields(rec), {"source_campaign"})
        self.assertEqual(contract_errors(rec), [])  # JSON válido = rescued data
        raw = self.payload[f"orders_{BATCH}.jsonl"].decode("utf-8")
        self.assertIn('"source_campaign":"SYN-CAMP-01"', raw)

    def test_version_conflict_distinct_payloads(self):
        info = self.snapshot_info
        conflict_versions = [(k[0], k[2]) for k in info["conflicted_keys"]]
        self.assertIn(("ORD-CONF-01", 2), conflict_versions)
        recs_v2 = [r for r in self.recs
                   if r["order_id"] == "ORD-CONF-01" and r["record_version"] == 2]
        self.assertEqual(len(recs_v2), 2)
        self.assertNotEqual(recs_v2[0], recs_v2[1])
        # Sin fila Silver vigente para la clave en conflicto (ORD-002).
        self.assertNotIn("ORD-CONF-01", {r["order_id"] for r in info["snapshot"]})


class GoldMetricsManual(unittest.TestCase):
    """Aceptación Fase 0 #1: métricas calculables a mano sobre el fixture.

    Cálculo manual SIN gating (seed 42, batch DEMO-001), snapshot por clave
    con mayor record_version válida y referencias de maestros válidas:
      celda A (2026-01-10, paddle, ES-MD, web):
        A01 220.00 (2x PRD-PALA-02, CUS-001)
        A02 165.00 (3x PRD-PALA-02, CUS-003)
        A03 165.00 (3x PRD-PALA-02, CUS-006)
        A04  30.00 (1x PRD-PALA-01, CUS-008)
        A05  60.00 (2x PRD-PALA-01, CUS-009) -> 640.00
      celda B (2026-01-11, rubber, ES-MD, web):
        B01 12.50 + B02 37.50 + B03 50.00 -> 100.00
      escenarios: DUP 12.50 + CORR v2 36.00 + REF-01 v2 neto 15.00
                  + LATE 5.80 + EXTRA 2.90 = 72.20
      total_revenue_eur sin gating             = 812.20
      order_count (count distinct con net > 0):
      8 baseline + DUP,CORR,REF-01,LATE,EXTRA  =  13
      refunded_line_count: REF-01 v2 (partial) + REF-02 v2 (full) = 2
    Las medidas PUBLICADAS (con gating PUB-001) se verifican aparte en
    GoldCellPrivacyGating: solo la celda A con 5 sujetos.
    """

    @classmethod
    def setUpClass(cls):
        cls.payload = _regen("gold", SEED)
        cls.info = resolve_snapshot(cls.payload)
        cls.metrics = gold_metrics(cls.info["snapshot"])

    def test_total_revenue_is_manual_value(self):
        self.assertEqual(self.metrics["total_revenue_eur"], _money(81220))
        self.assertIsInstance(self.metrics["total_revenue_eur"], Decimal)

    def test_scenario_subtotal_is_manual_value(self):
        scenario_ids = {"ORD-DUP-01", "ORD-CORR-01", "ORD-REF-01",
                        "ORD-LATE-01", "ORD-EXTRA-01"}
        subtotal = sum(
            (Decimal(r["net_revenue_eur"]) for r in self.info["snapshot"]
             if r["order_id"] in scenario_ids),
            Decimal("0"),
        )
        self.assertEqual(subtotal, _money(7220))
        # Sustitución correcta: la v1 PENDING de la CORR no aporta; el v2 sí.
        self.assertNotIn("ORD-CORR-01", [r["order_id"] for r in self.info["invalid"]])

    def test_order_count_count_distinct(self):
        self.assertEqual(self.metrics["order_count"], 13)
        eligible = {r["order_id"] for r in self.info["snapshot"]
                    if r["status"] in ("COMPLETED", "PARTIALLY_REFUNDED")
                    and Decimal(r["net_revenue_eur"]) > 0}
        self.assertEqual(len(eligible), 13)

    def test_refunded_line_count(self):
        self.assertEqual(self.metrics["refunded_line_count"], 2)

    def test_invalid_or_partial_never_reaches_gold(self):
        excluded = {r["order_id"] for r in self.info["snapshot"]}
        for bad in ("ORD-NULL-01", "ORD-ORPH-01", "ORD-CONF-01"):
            self.assertNotIn(bad, excluded)
        # Canceladas y reembolsadas completas aportan 0 al snapshot de ingresos.
        zero_net = [r for r in self.info["snapshot"]
                    if r["order_id"] in ("ORD-CANC-01", "ORD-REF-02")]
        self.assertEqual(len(zero_net), 2)
        for rec in zero_net:
            self.assertEqual(Decimal(rec["net_revenue_eur"]), Decimal("0.00"))

    def test_snapshot_uses_latest_valid_record_version(self):
        versions = {}
        for r in self.info["snapshot"]:
            key = (r["order_id"], r["line_id"])
            versions.setdefault(key, set()).add(r["record_version"])
        self.assertTrue(all(len(v) == 1 for v in versions.values()))
        # sanity: 8 claves baseline + 7 de escenarios = 15 claves vigentes
        self.assertEqual(len(versions), 15)


class GoldCellPrivacyGating(unittest.TestCase):
    """PUB-001: celda canónica (business_date, category, region_code, channel)
    con privacy_subject_count >= k=5 publicada; k<5 suprimida íntegra (ninguna
    medida y sin rollups). Agrupación REAL usando el maestro de productos.

    Control manual (seed 42):
      Celda A publicada: (2026-01-10, paddle, ES-MD, web), sujetos =
        {CUS-001, CUS-003, CUS-006, CUS-008, CUS-009} (5) ->
        total_revenue_eur = 640.00, order_count = 5, refunded_line_count = 0.
      Celda B suprimida: (2026-01-11, rubber, ES-MD, web), 3 sujetos ->
        sus 100.00 no se publican por ninguna medida.
      Celdas de escenarios = 1 sujeto -> suprimidas.
    """

    @classmethod
    def setUpClass(cls):
        cls.payload = _regen("gating", SEED)
        cls.info = resolve_snapshot(cls.payload)
        cls.published, cls.suppressed = gate_cells(cls.info, cls.payload)
        cls.measures = published_measures(cls.published)

    def test_published_cell_is_baseline_a_with_five_distinct_subjects(self):
        cell_a = (B_A, "paddle", "ES-MD", "web")
        self.assertIn(cell_a, self.published, list(self.published))
        # la celda publicada contiene los 5 pedidos del segmento A
        orders = {r["order_id"] for r in self.published[cell_a]["rows"]}
        self.assertEqual(orders, {f"ORD-BASE-A{i:02d}" for i in range(1, 6)})
        self.assertEqual(len(self.published[cell_a]["subjects"]), 5)
        self.assertEqual(self.published[cell_a]["subjects"],
                         {"CUS-001", "CUS-003", "CUS-006", "CUS-008", "CUS-009"})

    def test_baseline_b_cell_suppressed_with_three_subjects(self):
        cell_b = (B_B, "rubber", "ES-MD", "web")
        self.assertIn(cell_b, self.suppressed)
        self.assertEqual(len(self.suppressed[cell_b]["subjects"]), 3)
        self.assertEqual(
            {r["order_id"] for r in self.suppressed[cell_b]["rows"]},
            {f"ORD-BASE-B{i:02d}" for i in range(1, 4)},
        )

    def test_scenario_cells_suppressed_singleton(self):
        # cada celda de escenario vive sola con 1 sujeto -> PUB-001 la suprime
        suppressed_orders = set()
        for entry in self.suppressed.values():
            suppressed_orders |= {r["order_id"] for r in entry["rows"]}
        for scenario_id in ("ORD-DUP-01", "ORD-CORR-01", "ORD-REF-01",
                            "ORD-REF-02", "ORD-LATE-01", "ORD-EXTRA-01",
                            "ORD-CANC-01"):
            self.assertIn(scenario_id, suppressed_orders, scenario_id)
        # y ninguna fila de la celda publicable A aparece como suprimida
        self.assertNotIn("ORD-BASE-A01", suppressed_orders)
        self.assertNotIn("ORD-BASE-A05", suppressed_orders)

    def test_suppressed_cell_contributes_no_measures_and_no_rollups(self):
        # Toda medida publicable proviene únicamente de celdas con k sujetos.
        self.assertTrue(len(self.published) >= 1)
        expected_orders = set()
        for entry in self.published.values():
            for rec in entry["rows"]:
                if rec["status"] in ("COMPLETED", "PARTIALLY_REFUNDED") \
                        and Decimal(rec["net_revenue_eur"]) > 0:
                    expected_orders.add(rec["order_id"])
        # el revenue publicado no contiene ingresos de celdas suprimidas:
        # sumas manuales: celda B (100.00) y escenarios (89.70) quedan fuera;
        # los dos reembolsos (REF-01/REF-02) también viven en celdas
        # suprimidas -> refunded_line_count publicado es 0, no 2.
        self.assertEqual(self.measures["refunded_line_count"], 0)
        self.assertEqual(self.measures["order_count"], 5)
        self.assertEqual(self.measures["total_revenue_eur"], _money(64000))
        # ninguna clave vigente de celdas suprimidas entra en measures
        published_orders = set()
        for entry in self.published.values():
            published_orders |= {r["order_id"] for r in entry["rows"]}
        for rid in ("ORD-BASE-B01", "ORD-BASE-B02", "ORD-BASE-B03",
                    "ORD-DUP-01", "ORD-REF-01", "ORD-REF-02"):
            self.assertNotIn(rid, published_orders)

    def test_cells_use_real_four_dimension_grouping(self):
        # la agrupación usa las 4 dimensiones canónicas; la categoría viene del
        # maestro de productos: la celda paddle y la rubber quedan separadas.
        grouped_cells = set(self.published) | set(self.suppressed)
        self.assertIn((B_A, "paddle", "ES-MD", "web"), grouped_cells)
        self.assertIn((B_B, "rubber", "ES-MD", "web"), grouped_cells)
        # cada celda tiene un único (business_date, category, region, channel)
        products = _load_master(self.payload[f"products_{BATCH}.csv"])
        for cell, entry in {**self.published, **self.suppressed}.items():
            for rec in entry["rows"]:
                self.assertEqual(
                    cell,
                    (rec["business_date"],
                     products[rec["product_id"]]["category"],
                     rec["region_code"], rec["channel"]),
                )

    def test_invalid_masters_quarantined(self):
        # CUS-001 nulo rechazado: cliente cuarentenado; pedidos que lo
        # referencian no se promueven (no hay ninguno: pool del baseline lo
        # excluye). Catálogo PRD-001: sin productos inválidos en el fixture.
        self.assertIn("CUS-NULL-EMAIL", self.info["invalid_customers"])
        self.assertEqual(self.info["invalid_products"], set())


class BatchIdSecurity(unittest.TestCase):
    """El batch_id no puede recorrer rutas ni escapar del directorio de salida."""

    def test_invalid_batch_ids_raise(self):
        bad_ids = ["..", "../x", "a/b", "a\\b", "..\\x", "", " ", "DE|MO", "%2e%2e"]
        for bad in bad_ids:
            with self.assertRaises(SystemExit, msg=bad):
                gen.cli(["--seed", "1", "--batch-id", bad,
                         "--output", "data/generated"])

    def test_traversal_does_not_escape_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_root = Path(tmp) / "out"
            with self.assertRaises(SystemExit):
                gen.cli(["--seed", "1", "--batch-id", "..\\", "--output", str(out_root)])
            with self.assertRaises(SystemExit):
                gen.cli(["--seed", "1", "--batch-id", "../x", "--output", str(out_root)])
            # no se creó nada en absoluto (ni dentro ni fuera del output)
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_valid_custom_batch_writes_custom_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            rc = gen.cli(["--seed", "7", "--batch-id", "SL-2026",
                          "--output", str(tmp)])
            self.assertEqual(rc, 0)
            self.assertTrue((Path(tmp) / "SL-2026" / "manifest_SL-2026.json").exists())
            # manifest con metadata del lote correcto
            man = json.loads((Path(tmp) / "SL-2026" / "manifest_SL-2026.json")
                             .read_text("utf-8"))
            self.assertEqual(man["batch_id"], "SL-2026")
            self.assertEqual(man["seed"], 7)


class FixtureReproducibility(unittest.TestCase):
    """El fixture data/sample/DEMO-001 se reproduce byte a byte con seed 42."""

    def test_fixture_regenerates_identically(self):
        with tempfile.TemporaryDirectory() as tmp:
            gen.write_payload(_regen(tmp, SEED, BATCH), tmp)
            for name in (f"orders_{BATCH}.jsonl", f"customers_{BATCH}.csv",
                         f"products_{BATCH}.csv", f"manifest_{BATCH}.json"):
                regen = (Path(tmp) / BATCH / name).read_bytes()
                fixture = (FIXTURE_DIR / name).read_bytes()
                self.assertEqual(regen, fixture, name)

    def test_fixture_matches_declared_seed_and_manifest(self):
        man_file = FIXTURE_DIR / f"manifest_{BATCH}.json"
        self.assertTrue(man_file.exists(), "el fixture debe estar commiteado")
        man = json.loads(man_file.read_text("utf-8"))
        self.assertEqual(man["seed"], SEED)
        self.assertEqual(man["batch_id"], BATCH)
        for entry in man["files"]:
            data = (FIXTURE_DIR / entry["file"]).read_bytes()
            self.assertEqual(entry["sha256"], hashlib.sha256(data).hexdigest())


class CliSubprocess(unittest.TestCase):
    """El CLI funciona como subproceso (en local se ejecuta con `py`, que en
    Windows mapea a python.exe; en tests usamos sys.executable para portar)."""

    def test_cli_subprocess(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [sys.executable, str(SCRIPT), "--seed", "42", "--batch-id",
                 "DEMO-001", "--output", tmp],
                capture_output=True, text=True, cwd=str(ROOT),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            for name in (f"orders_{BATCH}.jsonl", f"customers_{BATCH}.csv",
                         f"products_{BATCH}.csv", f"manifest_{BATCH}.json"):
                regen = (Path(tmp) / BATCH / name).read_bytes()
                self.assertEqual(
                    regen, (FIXTURE_DIR / name).read_bytes(),
                    f"CLI no reproduce el fixture: {name}",
                )


if __name__ == "__main__":
    unittest.main()
