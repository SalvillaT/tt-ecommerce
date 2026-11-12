# 04 — Unity Catalog y gobierno: mínimo privilegio con IaC

**Tesis:** los permisos se declaran en Terraform, se revisan en gates humanos y se verifican con pruebas locales; el runtime recibe exactamente los privilegios que necesita y nada más.

## Diseño de gobierno

| Capa | Decisión | Dónde está declarado |
| --- | --- | --- |
| Almacenamiento gestionado | Aislado a nivel de catálogo, en un child path exclusivo bajo el contenedor dedicado (nunca el root DEFAULT del metastore ni el external location `lakehouse`) | [`infra/databricks/main.tf`](../../infra/databricks/main.tf) |
| Schemas medallion | `bronze`, `silver`, `quarantine`, `gold` creados por IaC, heredando el storage del catálogo | ídem |
| Grants del escritor humano | Grupo escritor preexistente: `USE_CATALOG`; `USE_SCHEMA` + `CREATE_TABLE` **solo** en `silver`/`quarantine`/`gold`; en `bronze`, solo `USE_SCHEMA` | ídem, validado por [`tests/test_phase2_terraform.py`](../../tests/test_phase2_terraform.py) |
| Run-as Bronze | SP dedicado del job de ingesta: **único** primer creador/propietario de las tablas raw y del run-log; Terraform solo le declara `CREATE_TABLE` sobre `bronze`, sin precreación | ídem |
| Run-as Silver | **SP dedicado separado** del de Bronze: `USE_CATALOG`; `USE_SCHEMA` en `bronze` (sin `CREATE_TABLE` — la escritura de Bronze es del run-as de ingesta); `SELECT` solo sobre las tres tablas raw exactas (nunca sobre el run-log ni a nivel de schema/catálogo); `USE_SCHEMA` + `CREATE_TABLE` en `silver`/`quarantine`/`gold` | [`infra/databricks/main.tf`](../../infra/databricks/main.tf) (recursos `phase3_silver_job_*`), validado por [`tests/test_phase3_terraform.py`](../../tests/test_phase3_terraform.py) |
| Identidades | Terraform **solo busca** los SP existentes por application ID: no crea, no gestiona, no versiona secretos | ídem |
| Gates humanos | Variables `_confirmed`/`_approved` como precondiciones que bloquean el apply sin atestación explícita del operador | ídem |
| Irreversibilidad | `prevent_destroy` en los recursos persistentes; ningún flag se resetea tras un apply | ídem |

## Del diseño al runtime (trazabilidad)

1. **Fase 1** (según reporte del operador): catálogo `tt_ecommerce_synthetic` con managed storage aislado, cuatro schemas vacíos y grants mínimos del grupo escritor, verificados en lectura.
2. **Fase 2** (aplicada en el ciclo cloud reportado por el operador; gates sin firmar): fundación de ingesta — credencial UC del conector landing, external locations de subruta de solo lectura (sin `_staging`), volumen `pipeline_state.ingestion_state` y `CREATE_TABLE` de `bronze` para el run-as del job.
3. **Fase 3** (commiteada en `983e7c0` y aplicada según reporte del operador): **9 altas; 0 cambios; 0 destrucciones** — los grants de mínimo privilegio del SP dedicado de Silver que habilitaron el hito.

Mapa detallado por root: [`infra/codemap.md`](../../infra/codemap.md) y [`infra/databricks/codemap.md`](../../infra/databricks/codemap.md).

## Principio aplicado: identidad = propósito

- Cada pipeline tiene **su propia identidad** con la superficie mínima: el SP Silver no puede escribir Bronze, no puede leer el run-log de ingesta, no tiene `SELECT` a nivel de schema y no toca landing ni `pipeline_state`.
- El propietario UC humano solo mantiene la relación de supervisión sobre el SP (rol de usuario de service principal); no se declara ningún acceso de plano de datos por esa vía.
- El job queda fuera de `_staging`: la subida y promoción del lote son exclusivas del productor/operador.
- Las tablas de salida las **crea el job** en su primera ejecución (primera escritura = propiedad), no un administrador ni Terraform.

## Lo que esta documentación NO reclama en gobierno

- **No** hay máscaras dinámicas de columna configuradas (trabajo de Fase 4).
- **No** hay prueba de identidad de consumidor (analistas/BI leyendo con permisos reales): la matriz de Fase 4 no se ha ejecutado.
- **No** se declara que el IaC sea la única fuente de acceso efectivo: grants heredados/preexistentes y propietarios pueden prevalecer, y así se documenta.
- El **SQL Warehouse permanece como código opt-in desactivado** (Fase 5, con gate de coste); no se ha creado ni arrancado.

## Condición de la evidencia

Los applies de Terraform y las verificaciones UC en runtime son **evidencia reportada por el operador**: no reproducibles desde Git ni audit-formales. Lo commiteado —el diseño declarativo, los gates y las suites que fijan la separación de privilegios— es verifiable localmente con:

```powershell
py -m unittest discover -s tests -v   # 287 pruebas, incluye test_phase2_terraform y test_phase3_terraform
```

Siguiente: [05-limites-y-siguientes-pasos.md](05-limites-y-siguientes-pasos.md).
