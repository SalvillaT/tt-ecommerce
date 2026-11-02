#!/usr/bin/env python3
"""Fase 0 — Generador de datos sintéticos (solo stdlib Python 3.13).

Contratos de referencia: docs/fase-0/CONTRATOS.md, docs/fase-0/METRICAS_Y_CALIDAD.md,
docs/fase-0/LINAJE_Y_PRIVACIDAD.md.

Genera un lote determinista para un par (seed, batch_id):

    orders_<batch_id>.jsonl   versionado por evento (order_id, line_id); incluye
                              escenarios negativos deliberados y aislados.
    customers_<batch_id>.csv  maestro de clientes versionado.
    products_<batch_id>.csv   maestro de productos versionado.
    manifest_<batch_id>.json  manifest con contract_version, generator_version,
                              batch_id, seed, scenarios, files (filas + SHA-256 de los
                              bytes) y total_rows. Sin timestamps de reloj, sin rutas
                              absolutas, sin rutas dependientes del OS.

Uso:
    py scripts/generate_synthetic_data.py --seed 42 --batch-id DEMO-001 --output data/generated

Determinismo: con igual generator_version, seed y batch_id los bytes son idénticos
(orden de filas, serialización, LF, manifest incluido). La seed altera materialmente
el baseline (producto de la misma categoría y cantidad); el resto de escenarios es
fijo e identificable por IDs deterministas (ORD-*).

Privacidad Gold (PUB-001): el baseline se construye para probar el gating de la
celda canónica grano (business_date, category, region_code, channel):
  - Segmento A: 5 pedidos COMPLETED de 5 customer_id distintos, todos en una
    única celda (2026-01-10, paddle, ES-MD, web) -> publica (k=5).
  - Segmento B: 3 pedidos COMPLETED de 3 customer_id distintos en otra celda
    (2026-01-11, rubber, ES-MD, web) -> celda suprimida (3 sujetos < k).
Los escenarios de reglas de calidad se emiten en celdas propias y aisladas.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import random
import re
import sys
from pathlib import Path

CONTRACT_VERSION = "v1"
GENERATOR_VERSION = "1.0.0"

# Un batch_id seguro no puede contener separadores de ruta ni recorrer directorios.
BATCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")

# ---- Fechas fijas del lote (sin reloj) -------------------------------------
# business_date del lote: 2026-01-10 / 2026-01-11; el tardío usa 2026-01-05.
B_A = "2026-01-10"
B_B = "2026-01-11"
B_LATE = "2026-01-05"  # evento tardío: fecha de negocio anterior al lote

CUSTOMERS = [
    # (id, region, email, phone o None para nulo permitido)
    ("CUS-001", "ES-MD", "ana.001@example.test", "+34000000001"),
    ("CUS-002", "ES-CT", "bruno.002@example.test", None),
    ("CUS-003", "ES-MD", "clara.003@example.test", None),
    ("CUS-004", "ES-VL", "diego.004@example.test", "+34000000004"),
    ("CUS-005", "ES-CT", "eva.005@example.test", None),
    ("CUS-006", "ES-MD", "fede.006@example.test", "+34000000006"),
    ("CUS-007", "ES-VC", "gina.007@example.test", None),
    ("CUS-008", "ES-MD", "hugo.008@example.test", "+34000000009"),
    ("CUS-009", "ES-MD", "iris.009@example.test", "+34000000010"),
    ("CUS-SOS-NOPHONE", "ES-VC", "lara.nophone@example.test", None),  # allowed_null
    ("CUS-NULL-EMAIL", "ES-MD", "", "+34000000008"),  # rejected_null (CUS-001)
]

PRODUCTS = [
    # (id, nombre, marca, categoria, precio en céntimos)
    ("PRD-PALA-01", "Control Pro", "TT Demo", "paddle", 3000),
    ("PRD-PALA-02", "Power X", "TT Demo", "paddle", 5500),
    ("PRD-RUB-01", "Spin Red", "TT Demo", "rubber", 1250),
    ("PRD-BAL-01", "Match Balls", "TT Demo", "ball", 290),
    ("PRD-TAB-01", "Pro Table", "TT Demo", "table", 13000),
    ("PRD-NET-01", "Net Set", "TT Demo", "net", 1800),
    ("PRD-ACC-01", "Grip Tape", "TT Demo", "accessory", 625),
]

PRODUCT_PRICE = {p[0]: p[4] for p in PRODUCTS}
PRODUCT_IDS = [p[0] for p in PRODUCTS]
CUSTOMER_REGION = {c[0]: c[1] for c in CUSTOMERS}
# Pool de clientes válidos (maestro vigente) para asignar al baseline por seed.
CUSTOMER_POOL = [c[0] for c in CUSTOMERS if c[0] != "CUS-NULL-EMAIL"]

MASTER_TS = "2026-01-01T08:00:00Z"

CUSTOMER_HEADER = [
    "customer_id", "record_version", "updated_at",
    "region_code", "email", "phone", "deleted_at",
]
PRODUCT_HEADER = [
    "product_id", "record_version", "updated_at",
    "name", "brand", "category", "list_price_eur", "currency", "active",
]

# Línea JSONL malformada deliberada (SCH-002), aislada al final del archivo.
MALFORMED_LINE = '{"order_id":"ORD-MALF-01","line_id":"1","record_version":1,'


def money(cents: int) -> str:
    """Céntimos -> cadena decimal(18,2) exacta ("60.00"). No usa float."""
    if cents < 0:
        raise ValueError("importe negativo no permitido")
    return f"{cents // 100}.{cents % 100:02d}"


def order_line(
    order_id: str,
    line_id: str,
    record_version: int,
    event_at: str,
    business_date: str,
    customer_id,
    product_id: str,
    quantity: int,
    status: str,
    *,
    refund_cents: int = 0,
    note=None,
    extra=None,
    channel: str = "web",
    region_code: str = "ES-MD",
) -> dict:
    """Construye un registro de orden según el contrato v1 (centavos exactos)."""
    unit = PRODUCT_PRICE[product_id]
    gross = unit * quantity
    if status == "COMPLETED":
        if refund_cents:
            raise ValueError("COMPLETED exige refund 0")
        net = gross
    elif status == "PARTIALLY_REFUNDED":
        if not (0 < refund_cents < gross):
            raise ValueError("PARTIALLY_REFUNDED exige 0 < refund < gross")
        net = gross - refund_cents
    elif status == "REFUNDED":
        if refund_cents != gross:
            raise ValueError("REFUNDED exige refund == gross")
        net = 0
    elif status in ("PENDING", "CANCELLED"):
        if refund_cents:
            raise ValueError("PENDING/CANCELLED no admite refund > 0")
        net = 0  # el bruto representa valor potencial, no venta realizada
    else:
        raise ValueError(f"estado no permitido: {status}")
    record = {
        "order_id": order_id,
        "line_id": line_id,
        "record_version": record_version,
        "event_at": event_at,
        "business_date": business_date,
        "customer_id": customer_id,
        "product_id": product_id,
        "status": status,
        "quantity": quantity,
        "unit_price_eur": money(unit),
        "gross_amount_eur": money(gross),
        "refund_amount_eur": money(refund_cents),
        "net_revenue_eur": money(net),
        "currency": "EUR",
        "channel": channel,
        "region_code": region_code,
        "note": note,
    }
    if extra:
        record.update(extra)  # campo JSON extra válido (SCH-001, rescued data)
    return record


def build_orders(seed: int, batch_id: str) -> list[str]:
    """Devuelve las líneas JSONL en orden determinista. Última = malformada."""
    records: list[dict] = []

    # Segmento baseline A: celda canónica publicable (k=5) —
    # (2026-01-10, paddle, ES-MD, web) con 5 customer_id distintos.
    # La seed varía producto dentro de la categoría paddle y cantidad.
    rng = random.Random(f"{GENERATOR_VERSION}|{seed}|{batch_id}")
    paddle_products = [pid for pid in PRODUCT_IDS
                       if next(p[3] for p in PRODUCTS if p[0] == pid) == "paddle"]
    cell_a_customers = ("CUS-001", "CUS-003", "CUS-006", "CUS-008", "CUS-009")
    for i, customer in enumerate(cell_a_customers):
        product = rng.choice(paddle_products)
        quantity = rng.randrange(1, 5)
        records.append(
            order_line(
                f"ORD-BASE-A{i + 1:02d}", "1", 1,
                f"{B_A}T10:{i * 7:02d}:00Z", B_A,
                customer, product, quantity, "COMPLETED",
                channel="web",
                region_code=CUSTOMER_REGION[customer],
            )
        )

    # Segmento baseline B: otra celda con menos sujetos que k (PUB-001 la
    # suprime íntegra) — (2026-01-11, rubber, ES-MD, web), 3 customer_id
    # distintos en su celda.
    cell_b_customers = ("CUS-003", "CUS-006", "CUS-008")
    for i, customer in enumerate(cell_b_customers):
        quantity = rng.randrange(1, 5)
        records.append(
            order_line(
                f"ORD-BASE-B{i + 1:02d}", "1", 1,
                f"{B_B}T13:{i * 11:02d}:00Z", B_B,
                customer, "PRD-RUB-01", quantity, "COMPLETED",
                channel="web",
                region_code=CUSTOMER_REGION[customer],
            )
        )

    # exact_duplicate: mismo registro emitido dos veces (ORD-006). Producto
    # rubber para que la línea viva en su propia celda canónica singleton
    # (2026-01-10, rubber, ES-MD, web) sin contaminar la celda A publicable.
    dup = order_line(
        "ORD-DUP-01", "1", 1, f"{B_A}T16:00:00Z", B_A,
        "CUS-003", "PRD-RUB-01", 1, "COMPLETED",  # gross 12.50
        channel="web", region_code="ES-MD",
    )
    records.append(dup)
    records.append(dup)  # byte-equivalente por construcción

    # later_correction: v1 PENDING corregida a v2 COMPLETED (sustituye a v1).
    records.append(order_line(
        "ORD-CORR-01", "1", 1, f"{B_A}T12:00:00Z", B_A,
        "CUS-001", "PRD-NET-01", 1, "PENDING",
        channel="web", region_code="ES-MD",
    ))
    records.append(order_line(
        "ORD-CORR-01", "1", 2, "2026-01-13T10:00:00Z", B_A,
        "CUS-001", "PRD-NET-01", 2, "COMPLETED",
        note="synthetic correction",
        channel="web", region_code="ES-MD",
    ))

    # cancellation: PENDING -> CANCELLED (refund 0, net 0).
    records.append(order_line(
        "ORD-CANC-01", "1", 1, f"{B_A}T13:00:00Z", B_A,
        "CUS-002", "PRD-ACC-01", 4, "PENDING",  # gross 25.00
        channel="marketplace", region_code="ES-CT",
    ))
    records.append(order_line(
        "ORD-CANC-01", "1", 2, "2026-01-14T09:00:00Z", B_A,
        "CUS-002", "PRD-ACC-01", 4, "CANCELLED",
        note="synthetic cancellation",
        channel="marketplace", region_code="ES-CT",
    ))

    # partial_refund: COMPLETED (55.00) -> PARTIALLY_REFUNDED (refund 40.00,
    # net 15.00).
    records.append(order_line(
        "ORD-REF-01", "1", 1, f"{B_A}T14:00:00Z", B_A,
        "CUS-004", "PRD-PALA-02", 1, "COMPLETED",  # gross 55.00
        channel="web", region_code="ES-VL",
    ))
    records.append(order_line(
        "ORD-REF-01", "1", 2, "2026-01-14T14:00:00Z", B_A,
        "CUS-004", "PRD-PALA-02", 1, "PARTIALLY_REFUNDED", refund_cents=4000,
        note="synthetic partial refund",
        channel="web", region_code="ES-VL",
    ))

    # full_refund: COMPLETED (130.00) -> REFUNDED (refund 130.00, net 0).
    records.append(order_line(
        "ORD-REF-02", "1", 1, f"{B_A}T15:00:00Z", B_A,
        "CUS-005", "PRD-TAB-01", 1, "COMPLETED",  # gross 130.00
        channel="web", region_code="ES-CT",
    ))
    records.append(order_line(
        "ORD-REF-02", "1", 2, "2026-01-14T15:00:00Z", B_A,
        "CUS-005", "PRD-TAB-01", 1, "REFUNDED", refund_cents=13000,
        note="synthetic full refund",
        channel="web", region_code="ES-CT",
    ))

    # late_event: válido, business_date muy anterior a event_at (fecha fija).
    records.append(order_line(
        "ORD-LATE-01", "1", 1, f"{B_A}T17:00:00Z", B_LATE,
        "CUS-006", "PRD-BAL-01", 2, "COMPLETED",  # gross 5.80
        note="synthetic late event",
        channel="web", region_code="ES-MD",
    ))

    # orphan_reference: cliente inexistente (ORD-005).
    records.append(order_line(
        "ORD-ORPH-01", "1", 1, f"{B_A}T18:00:00Z", B_A,
        "CUS-GHOST", "PRD-RUB-01", 3, "COMPLETED",  # gross 37.50
        channel="web", region_code="ES-MD",
    ))

    # extra_json_field: JSON válido con atributo desconocido (SCH-001).
    records.append(order_line(
        "ORD-EXTRA-01", "1", 1, f"{B_A}T19:00:00Z", B_A,
        "CUS-006", "PRD-BAL-01", 1, "COMPLETED",  # gross 2.90
        extra={"source_campaign": "SYN-CAMP-01"},
        channel="web", region_code="ES-MD",
    ))

    # rejected_null: campo obligatorio nulo (ORD-001) — JSON válido, clave nula.
    records.append(order_line(
        "ORD-NULL-01", "1", 1, f"{B_A}T20:00:00Z", B_A,
        None, "PRD-BAL-01", 1, "COMPLETED",  # gross 2.90
        channel="web", region_code="ES-MD",
    ))

    # version_conflict: misma clave+versión con payload distinto (ORD-002).
    records.append(order_line(
        "ORD-CONF-01", "1", 1, f"{B_A}T21:00:00Z", B_A,
        "CUS-007", "PRD-ACC-01", 5, "COMPLETED",  # gross 31.25
        channel="web", region_code="ES-VC",
    ))
    records.append(order_line(
        "ORD-CONF-01", "1", 2, "2026-01-12T08:00:00Z", B_A,
        "CUS-007", "PRD-ACC-01", 4, "COMPLETED",  # gross 25.00 (corrección)
        note="corrected quantity",
        channel="web", region_code="ES-VC",
    ))
    records.append(order_line(
        "ORD-CONF-01", "1", 2, "2026-01-12T09:00:00Z", B_A,
        "CUS-007", "PRD-ACC-01", 5, "COMPLETED",  # mismo v2, payload distinto
        note="conflicting quantity",
        channel="web", region_code="ES-VC",
    ))

    # malformed_json: una única línea no parseable, aislada al final (SCH-002).
    lines = [json.dumps(r, ensure_ascii=False, separators=(",", ":")) for r in records]
    lines.append(MALFORMED_LINE)
    return lines


def customer_rows() -> list[list[str]]:
    return [
        [cid, "1", MASTER_TS, region, email, phone or "", ""]
        for (cid, region, email, phone) in CUSTOMERS
    ]


def product_rows() -> list[list[str]]:
    return [
        [pid, "1", MASTER_TS, name, brand, category, money(price), "EUR", "true"]
        for (pid, name, brand, category, price) in PRODUCTS
    ]


def csv_bytes(header: list[str], rows: list[list[str]]) -> bytes:
    """CSV RFC 4180 (comillas según csv QUOTE_MINIMAL) con LF y UTF-8 sin BOM."""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def scenario_manifest(seed: int, batch_id: str) -> list[dict]:
    """Escenarios activados con sujeto determinista y regla de calidad asociada."""
    return [
        {"scenario": "baseline_valid", "rule": None,
         "subject": "ORD-BASE-A01..A05 | ORD-BASE-B01..B03"},
        {"scenario": "exact_duplicate", "rule": "ORD-006", "subject": "ORD-DUP-01"},
        {"scenario": "later_correction", "rule": None, "subject": "ORD-CORR-01"},
        {"scenario": "cancellation", "rule": None, "subject": "ORD-CANC-01"},
        {"scenario": "partial_refund", "rule": None, "subject": "ORD-REF-01"},
        {"scenario": "full_refund", "rule": None, "subject": "ORD-REF-02"},
        {"scenario": "late_event", "rule": None, "subject": "ORD-LATE-01"},
        {"scenario": "allowed_null", "rule": "CUS-002", "subject": "CUS-SOS-NOPHONE"},
        {"scenario": "rejected_null", "rule": "ORD-001|CUS-001",
         "subject": "ORD-NULL-01 | CUS-NULL-EMAIL"},
        {"scenario": "orphan_reference", "rule": "ORD-005", "subject": "ORD-ORPH-01"},
        {"scenario": "extra_json_field", "rule": "SCH-001", "subject": "ORD-EXTRA-01"},
        {"scenario": "malformed_json", "rule": "SCH-002", "subject": "ORD-MALF-01"},
        {"scenario": "version_conflict", "rule": "ORD-002", "subject": "ORD-CONF-01"},
    ]


def generate_payload(seed: int, batch_id: str) -> dict[str, bytes]:
    """Devuelve {nombre de archivo relativo: bytes} para el lote completo."""
    if not BATCH_ID_RE.fullmatch(batch_id):
        raise ValueError(
            f"batch_id no válido: {batch_id!r} (debe ser [A-Za-z0-9][A-Za-z0-9_-]*)"
        )
    orders = "\n".join(build_orders(seed, batch_id)) + "\n"
    payload: dict[str, bytes] = {
        f"orders_{batch_id}.jsonl": orders.encode("utf-8"),
        f"customers_{batch_id}.csv": csv_bytes(CUSTOMER_HEADER, customer_rows()),
        f"products_{batch_id}.csv": csv_bytes(PRODUCT_HEADER, product_rows()),
    }

    files = []
    total_rows = 0
    for name in (f"orders_{batch_id}.jsonl", f"customers_{batch_id}.csv",
                 f"products_{batch_id}.csv"):
        data = payload[name]
        rows = data.decode("utf-8").count("\n") - (1 if name.endswith(".csv") else 0)
        files.append({
            "file": name,
            "rows": rows,
            "sha256": hashlib.sha256(data).hexdigest(),
        })
        total_rows += rows

    manifest = {
        "contract_version": CONTRACT_VERSION,
        "generator_version": GENERATOR_VERSION,
        "batch_id": batch_id,
        "seed": seed,
        "scenarios": scenario_manifest(seed, batch_id),
        "files": files,
        "total_rows": total_rows,
    }
    payload[f"manifest_{batch_id}.json"] = (
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
    ).encode("utf-8")
    return payload


def write_payload(payload: dict[str, bytes], output: str) -> Path:
    """Escribe el lote en <output>/<batch_id>/ y devuelve esa ruta."""
    names = list(payload)
    batch_id = next(
        n[len("manifest_"):-len(".json")]
        for n in names if n.startswith("manifest_")
    )
    out = Path(output) / batch_id
    out.mkdir(parents=True, exist_ok=True)
    for name, data in payload.items():
        (out / name).write_bytes(data)
    return out


def cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generador de datos sintéticos Fase 0 (solo stdlib)."
    )
    parser.add_argument("--seed", type=int, required=True,
                        help="semilla determinista (int)")
    parser.add_argument("--batch-id", required=True,
                        help="identificador del lote, p. ej. DEMO-001")
    parser.add_argument("--output", default="data/generated",
                        help="directorio raíz de salida (default: data/generated)")
    args = parser.parse_args(argv)
    if not BATCH_ID_RE.fullmatch(args.batch_id):
        parser.error(
            f"batch-id no válido {args.batch_id!r}: debe casar "
            "'^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$' (sin separadores de ruta)."
        )
    payload = generate_payload(args.seed, args.batch_id)
    out = write_payload(payload, args.output)
    print(f"lote {args.batch_id} (seed={args.seed}) escrito en {out.as_posix()}")
    return 0


if __name__ == "__main__":
    sys.exit(cli())
