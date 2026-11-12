# 03 — Databricks Jobs y ejecución: bundle manual, dos tareas y controles exactos

## El job en una mirada

El job Silver del hito `DEMO-001` se define **declarativamente en un Databricks Asset Bundle** y se ejecuta **solo manualmente** (sin schedule):

| Propiedad | Valor | Por qué importa |
| --- | --- | --- |
| Definición | [`resources/silver_demo_001_job.yml`](../../resources/silver_demo_001_job.yml) + [`databricks.yml`](../../databricks.yml) | IaC del job: revisable, versionable, sin clicks en UI. |
| Ejecución | Manual únicamente, sin schedule | Sin compute fantasma ni costes recurrentes. |
| Concurrencia | `max_concurrent_runs: 1`, `max_retries: 0` | Un solo escritor por diseño. |
| Tareas | 2 ordenadas: `silver_materialization` → `silver_demo_001_verify` | La verificación read-only solo corre si la materialización terminó. |
| Run-as | Service principal **dedicado**, variable del bundle **sin default** (nunca un secreto del repo) | Separación de identidades; detalle en [04](04-unity-catalog-y-gobierno.md). |
| Parámetros de run | Solo `batch_id` y `audit_timestamp` | Catálogo, schemas, tablas y scope son **constantes del código**, no editables por run. |
| Timestamp de auditoría | Default fijo del bundle (`2026-01-20T12:00:00Z`) | Runs deterministas y reproducibles como evidencia. |
| Targets | `development_serverless` (por defecto) / `development_classic` (fallback autoterminando) | Cómputo efímero. |

## Qué hace cada tarea

1. **`silver_materialization`** ([`scripts/run_silver_demo_001_job.py`](../../scripts/run_silver_demo_001_job.py) → [`pipelines/silver/job.py`](../../pipelines/silver/job.py)): lee las tres tablas raw de Bronze **en solo lectura**, filtradas por `batch_id = DEMO-001`; ejecuta el núcleo puro ([`pipelines/silver/core.py`](../../pipelines/silver/core.py), stdlib sin PySpark, testeado con [`tests/test_silver_core.py`](../../tests/test_silver_core.py)); escribe las tablas gestionadas de salida —creadas y poseídas por el SP del job, nunca precreadas por Terraform— con `materialization_scope STRING NOT NULL` particionante y escrituras `replaceWhere` por scope ([`pipelines/silver/tables.py`](../../pipelines/silver/tables.py), [`pipelines/silver/spark.py`](../../pipelines/silver/spark.py)); al final escribe el reporte de completitud.
2. **`silver_demo_001_verify`** ([`scripts/verify_silver_demo_001.py`](../../scripts/verify_silver_demo_001.py)): **solo `SELECT`/`DESCRIBE` fijos** contra las salidas; JSON rojo-accionable determinista; sin parámetros de run por diseño; su lógica está testeada en [`tests/test_silver_verifier.py`](../../tests/test_silver_verifier.py) y las pruebas estáticas del bundle en [`tests/test_silver_job_bundle.py`](../../tests/test_silver_job_bundle.py).

## Resultado del run exitoso (operator-reported)

| Control | Resultado |
| --- | --- |
| Silver orders / customers / products | **15 / 10 / 7** filas |
| Cuarentena | **6 rechazos esperados** (5 de orders + 1 de customers, por las reglas del contrato) |
| Gold publicado | **una única celda privacy-safe**: pala / ES-MD / web = **640,00 EUR / 5 pedidos / 0 líneas reembolsadas** |
| Controles pre-supresión | **812,20 EUR / 13 / 2** |
| Estado del run report | **`COMPLETED_WITH_QUARANTINE`** (etiqueta explícita, no éxito limpio) |
| Verificador read-only | **todos los chequeos en verde** |
| PII en Gold | **ninguna** (sin identificadores ni contacto; la "PII" de origen del fixture es 100 % sintética — datos de prueba, no personas reales) |

El primer run de este job falló antes de tocar Bronze (incidencia de bootstrap resuelta): véase [02-correccion-del-pipeline.md](02-correccion-del-pipeline.md).

## Alcance exacto del hito (y lo que NO es)

- **Es**: una **instantánea completa y fija** del lote `DEMO-001`, materializada con `replaceWhere` **acotado por scope** (re-ejecutar reemplaza exactamente ese scope, sin duplicados; idempotente por scope) y timestamp de auditoría determinista.
- **No es**: streaming, CDC, SCD tipo 2, incremental genérico ni `MERGE`.
- **No es atómico entre tablas de salida**: una re-ejecución interrumpida puede dejar tablas en scopes distintos; el reporte final es el marcador de completitud.
- **No muta Bronze**: la lectura es read-only; Bronze quedó congelado tras su única ingesta.

## El ciclo Bronze equivalente

La ingesta Bronze (`resources/bronze_job.yml`) sigue el mismo patrón declarativo: job manual sin schedule, `landing_root` como **configuración de despliegue** (raíz exacta del contenedor, nunca parámetro del run), directorio de lote auditado antes de cualquier DDL, Auto Loader acotado al nombre de archivo aprobado y `CREATE_TABLE` sobre `bronze` reservado al run-as. Detalle normativo: [`docs/fase-2/ARQUITECTURA_Y_DECISIONES.md`](../fase-2/ARQUITECTURA_Y_DECISIONES.md).

## Condición de la evidencia

El despliegue manual del bundle, los runs y sus resultados son **evidencia reportada por el operador** (salidas de consola suministradas en la sesión de documentación): no reproducibles desde Git ni audit-formales. La definición del job, el código y sus pruebas sí son conmutados. Números de job/run concretos y límites completos: [acta de Fase 3](../fase-3/ACTA_EVIDENCIA_CLOUD_DEMO-001.md).

Siguiente: [04-unity-catalog-y-gobierno.md](04-unity-catalog-y-gobierno.md).
