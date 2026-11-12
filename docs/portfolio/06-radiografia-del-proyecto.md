# 06 — Radiografía del proyecto: qué hay, dónde y cómo se verifica

Mapa técnico completo, con rutas versionadas. Estado local verificable por cualquier lector: **287 pruebas `unittest` en 13 suites, en verde**.

```powershell
py -m unittest discover -s tests -v
```

## Stack

Python 3.13 (stdlib para el núcleo y el generador) · PySpark/Delta (ejecución diferida, solo workspace) · Databricks Asset Bundles + Workflows (serverless) · Unity Catalog · ADLS Gen2 · Terraform (3 roots) · unittest (sin PySpark local ni credenciales cloud).

## Mapa → claim → prueba

| Componente | Ruta versionada | Claim que soporta | Prueba local |
| --- | --- | --- | --- |
| Generador determinista + manifest | [`scripts/generate_synthetic_data.py`](../../scripts/generate_synthetic_data.py) | Datos reproducibles byte-exactos con anomalías deliberadas | [`tests/test_generate_synthetic_data.py`](../../tests/test_generate_synthetic_data.py) |
| Fixture `DEMO-001` | [`data/sample/DEMO-001/`](../../data/sample/DEMO-001/) | Lote canónico (26/11/7 líneas, escenarios `ORD-*`) con **campos con forma de PII 100 % sintéticos** (datos de prueba: dominios `*.example.test`, teléfonos ficticios) | anclado por las suites anteriores y [`tests/test_bronze_core.py`](../../tests/test_bronze_core.py) |
| Núcleo Bronze (contratos, layout, preflight, DDL, run-log) | [`pipelines/bronze/`](../../pipelines/bronze/) | Preflight estricto, append-only, propiedad del run-as, landing root como configuración | [`tests/test_bronze_core.py`](../../tests/test_bronze_core.py), [`tests/test_bronze_entrypoint.py`](../../tests/test_bronze_entrypoint.py) |
| CLI de promoción del lote | [`scripts/promote_batch.py`](../../scripts/promote_batch.py) | `plan`/`publish` con verificación SHA-256 y ready como última escritura | [`tests/test_bronze_promotion.py`](../../tests/test_bronze_promotion.py) |
| Root script Bronze + hardening | [`scripts/run_bronze_job.py`](../../scripts/run_bronze_job.py) | Directorio exacto auditado antes de DDL; `pathGlobFilter` al archivo aprobado | [`tests/test_bronze_remote_preflight.py`](../../tests/test_bronze_remote_preflight.py) |
| Job Bronze de verificación read-only | [`scripts/verify_bronze_demo_001.py`](../../scripts/verify_bronze_demo_001.py) | Verificación independiente post-ingesta | [`tests/test_bronze_verification_job.py`](../../tests/test_bronze_verification_job.py) |
| Bundle Fases 2–3 | [`databricks.yml`](../../databricks.yml), [`resources/bronze_job.yml`](../../resources/bronze_job.yml), [`resources/bronze_verification_job.yml`](../../resources/bronze_verification_job.yml), [`resources/silver_demo_001_job.yml`](../../resources/silver_demo_001_job.yml) | Jobs manuales, sin schedule, parámetros mínimos, run-as por variable sin default | [`tests/test_bronze_job_bundle.py`](../../tests/test_bronze_job_bundle.py), [`tests/test_silver_job_bundle.py`](../../tests/test_silver_job_bundle.py) |
| Núcleo Silver/Gold puro | [`pipelines/silver/core.py`](../../pipelines/silver/core.py) | Contrato, cuarentena explicable y gating k ≥ 5 sin PySpark | [`tests/test_silver_core.py`](../../tests/test_silver_core.py) |
| Boundary Silver (DDL, replaceWhere, reporte) | [`pipelines/silver/tables.py`](../../pipelines/silver/tables.py), [`pipelines/silver/spark.py`](../../pipelines/silver/spark.py), [`pipelines/silver/job.py`](../../pipelines/silver/job.py) | Escrituras acotadas por scope + marcador de completitud | [`tests/test_silver_spark.py`](../../tests/test_silver_spark.py) |
| Root script Silver + verificador | [`scripts/run_silver_demo_001_job.py`](../../scripts/run_silver_demo_001_job.py), [`scripts/verify_silver_demo_001.py`](../../scripts/verify_silver_demo_001.py) | Bootstrap workspace validado; verificación read-only determinista | [`tests/test_silver_verifier.py`](../../tests/test_silver_verifier.py) |
| IaC Azure (fundación) | [`infra/platform/`](../../infra/platform/) | Storage HNS, contenedores, workspace, conectores, RBAC acotado | `terraform validate` (host autorizado para `infra/databricks`) |
| IaC Unity Catalog (gobierno) | [`infra/databricks/main.tf`](../../infra/databricks/main.tf) | Managed storage aislado, grants mínimos, gates humanos, `prevent_destroy` | [`tests/test_phase2_terraform.py`](../../tests/test_phase2_terraform.py), [`tests/test_phase3_terraform.py`](../../tests/test_phase3_terraform.py) |
| Backend de estado | [`infra/bootstrap/`](../../infra/bootstrap/) | Estado remoto protegido, separado | `terraform validate` |
| Contratos y decisiones | [`docs/fase-0/`](../fase-0/), [`docs/fase-2/ARQUITECTURA_Y_DECISIONES.md`](../fase-2/ARQUITECTURA_Y_DECISIONES.md) | Semántica acordada antes de provisionar | — |
| Demo estático | [`demo/index.html`](../../demo/index.html) | Resultado visible sin compute (contenido fijo) | — |

## Suites (13) y qué cubren

1. [`test_generate_synthetic_data.py`](../../tests/test_generate_synthetic_data.py) — generador y fixture F0.
2. [`test_bronze_core.py`](../../tests/test_bronze_core.py) — preflight, DDL, writers append-only.
3. [`test_bronze_entrypoint.py`](../../tests/test_bronze_entrypoint.py) — parseo y validación de args del job.
4. [`test_bronze_job_bundle.py`](../../tests/test_bronze_job_bundle.py) — bundle Bronze.
5. [`test_bronze_promotion.py`](../../tests/test_bronze_promotion.py) — promoción plan/publish.
6. [`test_bronze_remote_preflight.py`](../../tests/test_bronze_remote_preflight.py) — auditoría remota del directorio exacto.
7. [`test_bronze_verification_job.py`](../../tests/test_bronze_verification_job.py) — job de verificación read-only Bronze.
8. [`test_silver_core.py`](../../tests/test_silver_core.py) — núcleo Silver/cuarentena/Gold.
9. [`test_silver_spark.py`](../../tests/test_silver_spark.py) — adaptadores y `replaceWhere`.
10. [`test_silver_verifier.py`](../../tests/test_silver_verifier.py) — verificador read-only Silver.
11. [`test_silver_job_bundle.py`](../../tests/test_silver_job_bundle.py) — bundle Silver + estática del verificador.
12. [`test_phase2_terraform.py`](../../tests/test_phase2_terraform.py) — separación de privilegios Fase 2 (run-as vs. escritor).
13. [`test_phase3_terraform.py`](../../tests/test_phase3_terraform.py) — mínimo privilegio del SP Silver.

## Estados, sin ambigüedad

| Elemento | Estado |
| --- | --- |
| Código, bundle, Terraform, fixture, tests | **Versionado y commiteado** (evidencia del repositorio). |
| 287 pruebas locales | **En verde**, sin credenciales cloud. |
| Ingesta Bronze `DEMO-001` | Ejecutada **una vez** en cloud; verificada en lectura; **Bronze congelado**. |
| Hito Silver `DEMO-001` | Ejecutado y verificado en cloud. |
| Cierre formal de Fase 2 | **Pendiente** (borrador por excepción sin firmar). |
| Evidencia cloud | **Operator-reported**, no reproducible desde Git. |
| SQL Warehouse, Power BI, dashboard, CI/CD, máscaras, prueba de consumidor | **Sin iniciar**. |

Documentación de detalle: [codemap raíz](../../codemap.md) · [acta de evidencia del hito Silver](../fase-3/ACTA_EVIDENCIA_CLOUD_DEMO-001.md) · [borrador de cierre de Fase 2 (sin firmar)](../fase-2/EVIDENCIA_CLOUD_DEMO-001_CIERRE_EXCEPCION_BORRADOR.md).

Volver al [índice del dossier](README.md).
