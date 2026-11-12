"""Dependency-free implementation of the Fase 0 Silver and Gold contracts.

Inputs are flattened Bronze-shaped mappings: source fields plus the audit fields
declared in :mod:`pipelines.bronze.contracts`.  This module performs no I/O.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import re
from typing import Any, Iterable, Mapping

from pipelines.bronze.contracts import (
    AUDIT_COLUMNS,
    CUSTOMERS_COLUMNS,
    ORDERS_COLUMNS,
    PRODUCTS_COLUMNS,
)

SOURCE_SYSTEM = "synthetic-generator-v1"
K_ANONYMITY_THRESHOLD = 5
_STATUSES = {"PENDING", "COMPLETED", "CANCELLED", "PARTIALLY_REFUNDED", "REFUNDED"}
_CATEGORIES = {"paddle", "rubber", "ball", "table", "net", "accessory"}
_CHANNELS = {"web", "marketplace"}
_TRANSITIONS = {
    ("PENDING", "COMPLETED"), ("PENDING", "CANCELLED"),
    ("COMPLETED", "PARTIALLY_REFUNDED"), ("COMPLETED", "REFUNDED"),
    ("PARTIALLY_REFUNDED", "PARTIALLY_REFUNDED"),
    ("PARTIALLY_REFUNDED", "REFUNDED"), ("COMPLETED", "COMPLETED"),
}
_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[a-z]+$")


@dataclass(frozen=True)
class BronzeLineage:
    batch_id: str
    contract_version: str
    manifest_file_sha256: str
    source_file: str
    source_file_size: object
    source_file_modification_time: object
    ingested_at: object
    pipeline_run_id: str


@dataclass(frozen=True)
class SilverRow:
    """One typed current entity row; ``business`` preserves every source field."""

    source: str
    business: tuple[tuple[str, object], ...]
    lineage: BronzeLineage
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None
    source_system: str
    quality_status: str

    def as_dict(self) -> dict[str, object]:
        return dict(self.business)


@dataclass(frozen=True)
class QuarantineRecord:
    source: str
    available_key: tuple[str, ...]
    rule_ids: tuple[str, ...]
    details: tuple[str, ...]
    batch_id: str
    source_file: str
    manifest_file_sha256: str
    payload_reference: str
    quarantined_at: datetime
    quality_status: str = "QUARANTINED"


@dataclass(frozen=True)
class GoldDailySalesSegment:
    business_date: date
    category: str
    region_code: str
    channel: str
    total_revenue_eur: Decimal
    order_count: int
    refunded_line_count: int
    refreshed_at: datetime
    source_system: str
    quality_status: str

    def as_dict(self) -> dict[str, object]:
        """JSON-safe and intentionally free of customer/order identifiers."""
        return {
            "business_date": self.business_date.isoformat(), "category": self.category,
            "region_code": self.region_code, "channel": self.channel,
            "total_revenue_eur": str(self.total_revenue_eur), "order_count": self.order_count,
            "refunded_line_count": self.refunded_line_count,
            "refreshed_at": self.refreshed_at.isoformat(), "source_system": self.source_system,
            "quality_status": self.quality_status,
        }


@dataclass(frozen=True)
class SuppressedCell:
    business_date: date
    category: str
    region_code: str
    channel: str
    reason: str = "PUB-001"

    def as_dict(self) -> dict[str, str]:
        return {"business_date": self.business_date.isoformat(), "category": self.category,
                "region_code": self.region_code, "channel": self.channel, "reason": self.reason}


@dataclass(frozen=True)
class QualityReport:
    by_rule: tuple[tuple[str, int], ...]
    by_source: tuple[tuple[str, int], ...]
    by_parse_status: tuple[tuple[str, int], ...]
    current_by_source: tuple[tuple[str, int], ...]
    quarantine_by_source: tuple[tuple[str, int], ...]
    published_cells: int
    suppressed_cells: int
    duplicate_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "by_rule": dict(self.by_rule), "by_source": dict(self.by_source),
            "by_parse_status": dict(self.by_parse_status),
            "current_by_source": dict(self.current_by_source),
            "quarantine_by_source": dict(self.quarantine_by_source),
            "published_cells": self.published_cells, "suppressed_cells": self.suppressed_cells,
            "duplicate_count": self.duplicate_count,
        }


@dataclass(frozen=True)
class PipelineResult:
    orders: tuple[SilverRow, ...]
    customers: tuple[SilverRow, ...]
    products: tuple[SilverRow, ...]
    quarantine: tuple[QuarantineRecord, ...]
    gold: tuple[GoldDailySalesSegment, ...]
    suppressed_cells: tuple[SuppressedCell, ...]
    quality_report: QualityReport
    pre_suppression_controls: tuple[tuple[str, object], ...]


@dataclass(frozen=True)
class _Candidate:
    source: str
    values: tuple[tuple[str, object], ...]
    lineage: BronzeLineage
    parse_status: str
    warning_rules: tuple[str, ...]
    payload_reference: str

    def value(self, name: str) -> object:
        return dict(self.values).get(name)

    def key(self) -> tuple[str, ...]:
        names = {"orders": ("order_id", "line_id"), "customers": ("customer_id",), "products": ("product_id",)}[self.source]
        return tuple(str(self.value(name) or "") for name in names)

    def version(self) -> int:
        return int(self.value("record_version"))


def run_pipeline(
    *, orders: Iterable[Mapping[str, object]], customers: Iterable[Mapping[str, object]],
    products: Iterable[Mapping[str, object]], audit_timestamp: datetime,
) -> PipelineResult:
    """Transform Bronze-shaped rows into current Silver, quarantine, and Gold.

    ``audit_timestamp`` is deliberately supplied by the caller for repeatable tests.
    It must be timezone-aware UTC.
    """
    now = _utc(audit_timestamp)
    quarantines: list[QuarantineRecord] = []
    rule_counts: Counter[str] = Counter()
    parse_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    duplicate_count = 0

    candidates: dict[str, list[_Candidate]] = {}
    for source, rows, fields in (("orders", orders, ORDERS_COLUMNS), ("customers", customers, CUSTOMERS_COLUMNS), ("products", products, PRODUCTS_COLUMNS)):
        parsed, qs = _parse_source(source, rows, fields, now, parse_counts, source_counts, rule_counts)
        candidates[source] = parsed
        quarantines.extend(qs)

    masters: dict[str, list[_Candidate]] = {}
    for source in ("customers", "products"):
        valid, qs, duplicates = _validate_and_resolve(source, candidates[source], now, rule_counts)
        masters[source] = valid
        quarantines.extend(qs)
        duplicate_count += duplicates

    valid_orders, qs, duplicates = _validate_and_resolve("orders", candidates["orders"], now, rule_counts)
    quarantines.extend(qs)
    duplicate_count += duplicates

    customer_ids = {c.value("customer_id") for c in masters["customers"]}
    product_ids = {p.value("product_id") for p in masters["products"]}
    current_orders: list[_Candidate] = []
    for row in valid_orders:
        missing = []
        if row.value("customer_id") not in customer_ids:
            missing.append("customer_id")
        if row.value("product_id") not in product_ids:
            missing.append("product_id")
        if missing:
            quarantines.append(_quarantine(row, ("ORD-005",), tuple(f"referencia inexistente o inválida: {x}" for x in missing), now))
            rule_counts["ORD-005"] += 1
        else:
            current_orders.append(row)

    silver_orders = _to_silver(current_orders, now)
    silver_customers = _to_silver(masters["customers"], now)
    silver_products = _to_silver(masters["products"], now)
    gold, suppressed, controls = _build_gold(current_orders, masters["products"], now)
    quarantine = tuple(sorted(set(quarantines), key=lambda q: (q.source, q.available_key, q.payload_reference, q.rule_ids)))
    q_by_source = Counter(q.source for q in quarantine)
    report = QualityReport(
        tuple(sorted(rule_counts.items())), tuple(sorted(source_counts.items())), tuple(sorted(parse_counts.items())),
        tuple(sorted(Counter(r.source for r in (*silver_orders, *silver_customers, *silver_products)).items())),
        tuple(sorted(q_by_source.items())), len(gold), len(suppressed), duplicate_count,
    )
    return PipelineResult(silver_orders, silver_customers, silver_products, quarantine, gold, suppressed, report, controls)


def _parse_source(source: str, rows: Iterable[Mapping[str, object]], fields: tuple[str, ...], now: datetime,
                  parse_counts: Counter[str], source_counts: Counter[str], rule_counts: Counter[str]) -> tuple[list[_Candidate], list[QuarantineRecord]]:
    parsed: list[_Candidate] = []
    quarantines: list[QuarantineRecord] = []
    for raw in rows:
        source_counts[source] += 1
        status = str(raw.get("parse_status", ""))
        parse_counts[status] += 1
        lineage = _lineage(raw)
        reference = _payload_reference(raw)
        if status not in {"PARSED", "RESCUED", "MALFORMED"}:
            raise ValueError(f"unknown parse_status for {source}: {status!r}")
        if status == "MALFORMED":
            candidate = _Candidate(source, tuple(), lineage, status, tuple(), reference)
            quarantines.append(_quarantine(candidate, ("SCH-002",), ("registro Bronze malformado",), now))
            rule_counts["SCH-002"] += 1
            continue
        try:
            values = tuple(sorted((field, _typed(source, field, raw.get(field))) for field in fields))
        except (ValueError, InvalidOperation) as exc:
            # Conversion errors are contract failures; the validator supplies the F0 rule.
            values = tuple(sorted((field, raw.get(field)) for field in fields))
        warnings = tuple(sorted(
            (("SCH-001",) if status == "RESCUED" else tuple())
            + (("CUS-002",) if source == "customers" and raw.get("phone") in (None, "") else tuple())
        ))
        for warning in warnings:
            rule_counts[warning] += 1
        parsed.append(_Candidate(source, values, lineage, status, warnings, reference))
    return parsed, quarantines


def _validate_and_resolve(source: str, rows: list[_Candidate], now: datetime,
                          rule_counts: Counter[str]) -> tuple[list[_Candidate], list[QuarantineRecord], int]:
    valid: list[_Candidate] = []
    quarantine: list[QuarantineRecord] = []
    for row in rows:
        failures = _validate(source, row)
        if failures:
            ids = tuple(sorted({item[0] for item in failures}))
            quarantine.append(_quarantine(row, ids, tuple(sorted(item[1] for item in failures)), now))
            rule_counts.update(ids)
        else:
            valid.append(row)

    grouped: dict[tuple[str, ...], list[_Candidate]] = defaultdict(list)
    for row in valid:
        grouped[row.key()].append(row)
    resolved: list[_Candidate] = []
    duplicates = 0
    for key in sorted(grouped):
        versions: dict[int, list[_Candidate]] = defaultdict(list)
        for row in grouped[key]:
            versions[row.version()].append(row)
        conflicted = False
        unique_versions: list[_Candidate] = []
        for version in sorted(versions):
            rows_at_version = versions[version]
            distinct = {r.values for r in rows_at_version}
            if len(distinct) > 1:
                conflicted = True
                for row in rows_at_version:
                    quarantine.append(_quarantine(row, ("ORD-002" if source == "orders" else _master_rule(source),),
                                                  ("misma clave y versión con payload distinto",), now))
                    rule_counts["ORD-002" if source == "orders" else _master_rule(source)] += 1
            else:
                unique_versions.append(sorted(rows_at_version, key=lambda r: r.payload_reference)[0])
                duplicate_rows = len(rows_at_version) - 1
                duplicates += duplicate_rows
                if source == "orders":
                    rule_counts["ORD-006"] += duplicate_rows
        if conflicted:
            # F0 orders contract explicitly removes the entire business key.  The same
            # fail-closed rule is intentionally used for masters.
            continue
        if source == "orders":
            transition_failures = _transition_failures(unique_versions)
            if transition_failures:
                for row in unique_versions:
                    quarantine.append(_quarantine(row, ("ORD-003",), tuple(transition_failures), now))
                    rule_counts["ORD-003"] += 1
                continue
        resolved.append(max(unique_versions, key=lambda r: r.version()))
    return resolved, quarantine, duplicates


def _validate(source: str, row: _Candidate) -> list[tuple[str, str]]:
    values = dict(row.values)
    if source == "orders":
        required = set(ORDERS_COLUMNS) - {"note"}
        errors = [("ORD-001", f"nulo o vacío obligatorio: {x}") for x in sorted(required) if values.get(x) in (None, "")]
        if errors:
            return errors
        try:
            if not isinstance(values["record_version"], int) or values["record_version"] < 1:
                errors.append(("ORD-002", "record_version debe ser entero positivo"))
            if values["status"] not in _STATUSES or values["currency"] != "EUR" or values["channel"] not in _CHANNELS:
                errors.append(("ORD-003", "estado, moneda o canal inválido"))
            if not isinstance(values["quantity"], int) or values["quantity"] <= 0:
                errors.append(("ORD-004", "quantity debe ser positiva"))
            amounts = [values[x] for x in ("unit_price_eur", "gross_amount_eur", "refund_amount_eur", "net_revenue_eur")]
            if any(not isinstance(x, Decimal) or x < 0 or x > Decimal("1000000") for x in amounts):
                errors.append(("ORD-004", "importe fuera de rango"))
            unit, gross, refund, net = amounts
            if unit * values["quantity"] != gross:
                errors.append(("ORD-003", "gross != quantity x unit_price"))
            status = values["status"]
            if status in {"PENDING", "CANCELLED"} and (refund != 0 or net != 0):
                errors.append(("ORD-003", "pending/cancelled requiere neto y refund cero"))
            if status not in {"PENDING", "CANCELLED"} and net != gross - refund:
                errors.append(("ORD-003", "net != gross - refund"))
            if status == "COMPLETED" and refund != 0:
                errors.append(("ORD-003", "completed requiere refund cero"))
            if status == "REFUNDED" and refund != gross:
                errors.append(("ORD-003", "refunded requiere refund igual a gross"))
            if status == "PARTIALLY_REFUNDED" and not (0 < refund < gross):
                errors.append(("ORD-003", "partial refund fuera de rango"))
            if len(values.get("note") or "") > 200:
                errors.append(("ORD-001", "note supera 200 caracteres"))
        except (KeyError, TypeError, InvalidOperation):
            errors.append(("ORD-001", "tipo o formato inválido"))
        return errors
    if source == "customers":
        required = ("customer_id", "record_version", "updated_at", "region_code", "email")
        errors = [("CUS-001", f"nulo o vacío obligatorio: {x}") for x in required if values.get(x) in (None, "")]
        if values.get("email") not in (None, "") and not _EMAIL.fullmatch(str(values["email"])):
            errors.append(("CUS-001", "email inválido"))
        if not isinstance(values.get("record_version"), int) or values.get("record_version", 0) < 1:
            errors.append(("CUS-001", "record_version inválido"))
        return errors
    required = set(PRODUCTS_COLUMNS)
    errors = [("PRD-001", f"nulo o vacío obligatorio: {x}") for x in sorted(required) if values.get(x) in (None, "")]
    if values.get("category") not in _CATEGORIES or values.get("currency") != "EUR" or not isinstance(values.get("active"), bool):
        errors.append(("PRD-001", "categoría, moneda o active inválido"))
    if not isinstance(values.get("record_version"), int) or values.get("record_version", 0) < 1:
        errors.append(("PRD-001", "record_version inválido"))
    price = values.get("list_price_eur")
    if not isinstance(price, Decimal) or price < 0:
        errors.append(("PRD-001", "list_price_eur inválido"))
    return errors


def _transition_failures(rows: list[_Candidate]) -> list[str]:
    ordered = sorted(rows, key=lambda r: r.version())
    return ["transición de estado no permitida" for before, after in zip(ordered, ordered[1:])
            if (str(before.value("status")), str(after.value("status"))) not in _TRANSITIONS]


def _to_silver(rows: Iterable[_Candidate], now: datetime) -> tuple[SilverRow, ...]:
    output = []
    for row in rows:
        values = dict(row.values)
        deleted = values.get("deleted_at") if row.source == "customers" else None
        quality = "VALID_WITH_WARNINGS" if row.warning_rules or (row.source == "customers" and values.get("phone") is None) else "VALID"
        output.append(SilverRow(row.source, row.values, row.lineage, now, now, deleted, SOURCE_SYSTEM, quality))
    return tuple(sorted(output, key=lambda r: (r.source, tuple(str(x) for x in r.as_dict().values()))))


def _build_gold(orders: list[_Candidate], products: list[_Candidate], now: datetime) -> tuple[tuple[GoldDailySalesSegment, ...], tuple[SuppressedCell, ...], tuple[tuple[str, object], ...]]:
    categories = {p.value("product_id"): p.value("category") for p in products}
    cells: dict[tuple[date, str, str, str], list[_Candidate]] = defaultdict(list)
    for row in orders:
        cells[(row.value("business_date"), str(categories[row.value("product_id")]), str(row.value("region_code")), str(row.value("channel")))].append(row)
    published: list[GoldDailySalesSegment] = []
    suppressed: list[SuppressedCell] = []
    all_revenue = Decimal("0.00")
    all_orders: set[object] = set()
    all_refunds = 0
    for cell, rows in sorted(cells.items()):
        eligible = [r for r in rows if r.value("status") in {"COMPLETED", "PARTIALLY_REFUNDED"} and r.value("net_revenue_eur") > 0]
        all_revenue += sum((r.value("net_revenue_eur") for r in eligible), Decimal("0.00"))
        all_orders.update(r.value("order_id") for r in eligible)
        all_refunds += sum(r.value("status") in {"REFUNDED", "PARTIALLY_REFUNDED"} for r in rows)
        subjects = {r.value("customer_id") for r in rows if r.value("status") in {"COMPLETED", "PARTIALLY_REFUNDED", "REFUNDED"}}
        if len(subjects) < K_ANONYMITY_THRESHOLD:
            suppressed.append(SuppressedCell(*cell))
            continue
        published.append(GoldDailySalesSegment(
            *cell, sum((r.value("net_revenue_eur") for r in eligible), Decimal("0.00")),
            len({r.value("order_id") for r in eligible}),
            sum(r.value("status") in {"REFUNDED", "PARTIALLY_REFUNDED"} for r in rows),
            now, SOURCE_SYSTEM, "VALID",
        ))
    controls = (("total_revenue_eur", all_revenue), ("order_count", len(all_orders)), ("refunded_line_count", all_refunds))
    return tuple(published), tuple(suppressed), controls


def _typed(source: str, field: str, value: object) -> object:
    if value in (None, ""):
        return None
    if field in {"record_version", "quantity"}:
        return int(value)
    if field in {"unit_price_eur", "gross_amount_eur", "refund_amount_eur", "net_revenue_eur", "list_price_eur"}:
        text = str(value)
        if not re.fullmatch(r"\d+\.\d{2}", text):
            raise ValueError("not decimal(18,2)")
        return Decimal(text)
    if field in {"event_at", "updated_at", "deleted_at"}:
        parsed = datetime.strptime(str(value), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return parsed
    if field == "business_date":
        return date.fromisoformat(str(value))
    if field == "active":
        if str(value).lower() not in {"true", "false"}:
            raise ValueError("not boolean")
        return str(value).lower() == "true"
    return value


def _lineage(raw: Mapping[str, object]) -> BronzeLineage:
    missing = [name for name in AUDIT_COLUMNS if name not in raw]
    if missing:
        raise ValueError(f"Bronze row misses audit fields: {', '.join(missing)}")
    return BronzeLineage(str(raw["batch_id"]), str(raw["contract_version"]), str(raw["manifest_file_sha256"]),
                         str(raw["_source_file"]), raw["_source_file_size"], raw["_source_file_modification_time"],
                         raw["_ingested_at"], str(raw["_pipeline_run_id"]))


def _payload_reference(raw: Mapping[str, object]) -> str:
    canonical = str(raw.get("parsed_payload_canonical") or raw.get("_corrupt_record") or "")
    return "sha256:" + sha256(canonical.encode("utf-8")).hexdigest()


def _quarantine(row: _Candidate, rule_ids: tuple[str, ...], details: tuple[str, ...], now: datetime) -> QuarantineRecord:
    return QuarantineRecord(row.source, row.key(), tuple(sorted(set(rule_ids))), tuple(sorted(set(details))),
                            row.lineage.batch_id, row.lineage.source_file, row.lineage.manifest_file_sha256,
                            row.payload_reference, now)


def _master_rule(source: str) -> str:
    return "CUS-001" if source == "customers" else "PRD-001"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("audit_timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)
