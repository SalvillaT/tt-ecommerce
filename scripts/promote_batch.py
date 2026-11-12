"""CLI operatorial de promoción de lotes (Fase 2, gate GF2-6).

Subcommandos:

* `plan`    — offline: valida el directorio local contra el manifest y emite
              el plan determinista en JSON. No toca Azure.
* `publish` — operatorial: ejecuta el plan contra la cuenta Azure indicada con
              `--account-name` (sin secretos; la identidad la aporta `az login`).
              Solo se ejecuta cuando el gate GF2-6 ya fue aprobado; jamás
              constituye autorización implícita.

Errores: exit code distinto de cero y JSON en stderr; `publish` nunca limpia
ni sobrescribe: ante fallo queda estado parcial que exige reconciliación humana.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.bronze.manifest import ManifestPreflightError  # noqa: E402
from pipelines.bronze.promotion import (  # noqa: E402
    AzCliRunner,
    LandingClient,
    PromotionError,
    build_promotion_plan,
    publish_promotion,
)


def _add_batch_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--batch-id", required=True,
                        help="Identificador del lote (p. ej. DEMO-001).")
    parser.add_argument("--arrival-date", required=True,
                        help="Fecha de llegada YYYY-MM-DD del layout canónico.")
    parser.add_argument("--local-batch-dir", required=True,
                        help="Directorio plano del lote (manifest + orders/customers/products).")


def _print_json_error(exc: BaseException) -> None:
    print(json.dumps({"error": type(exc).__name__, "message": str(exc)}), file=sys.stderr)


def _handle_plan(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    try:
        plan = build_promotion_plan(Path(args.local_batch_dir), args.batch_id, args.arrival_date)
    except (ValueError, ManifestPreflightError, PromotionError) as exc:
        _print_json_error(exc)
        return 1
    print(json.dumps(plan.to_dict(), indent=2))
    return 0


def _handle_publish(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    if not args.account_name:
        parser.error("publish requires --account-name; publish is only for the GF2-6 authorized run")
    try:
        plan = build_promotion_plan(Path(args.local_batch_dir), args.batch_id, args.arrival_date)
        client = LandingClient(args.account_name, AzCliRunner())
        events = publish_promotion(plan, client, local_batch_dir=Path(args.local_batch_dir),
                                   on_event=lambda event: print(json.dumps(event), flush=True))
    except (ValueError, ManifestPreflightError, PromotionError) as exc:
        _print_json_error(exc)
        return 1
    print(json.dumps({
        "status": "published",
        "batch_id": plan.batch_id,
        "ready": f"landing/_control/ready/{plan.batch_id}.json",
        "steps": len(events),
    }))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="promote_batch.py",
        description="Planifica o publica la promoción de un lote hacia landing (Fase 2, GF2-6).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan_parser = subparsers.add_parser("plan", help="Plan determinista offline en JSON (sin Azure).")
    _add_batch_arguments(plan_parser)
    publish_parser = subparsers.add_parser(
        "publish", help="Ejecuta el plan con Azure CLI; exige --account-name (run GF2-6).")
    _add_batch_arguments(publish_parser)
    publish_parser.add_argument(
        "--account-name", required=True,
        help="Nombre de la Storage Account ADLS Gen2 (sin claves; auth vía az login).")

    args = parser.parse_args(argv)
    if args.command == "plan":
        return _handle_plan(args, parser)
    return _handle_publish(args, parser)


if __name__ == "__main__":
    sys.exit(main())
