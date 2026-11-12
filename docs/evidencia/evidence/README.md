# Evidencia visual — plan de captura (PENDIENTE DE OPERADOR)

Las capturas de pantalla de esta documentación **no existen todavía**. Deben ser capturadas **manualmente por un usuario autenticado** del workspace, redactadas antes de versionar y etiquetadas como **`operator-reported`** (son capturas de una ejecución ya realizada, no re-ejecuciones ni consultas en directo). En esta carpeta solo hay un `.gitkeep`: **nunca** se versiona una imagen provisional o falsa.

Reglas no negociables:

1. **Captura manual autenticada**: cada imagen se toma desde la consola de Databricks con una sesión autorizada del operador, sobre el estado ya documentado del run. No se simula UI, no se re-ejecuta nada para "regenerar" la evidencia y no se fabrican datos.
2. **Redacción antes de commit**: antes de versionar cualquier PNG, se ocultan todos los identificadores y URLs sensibles (lista por captura abajo y en [`evidence-index.yml`](evidence-index.yml)). Si una redacción no es viable (el identificador es parte del resultado a mostrar), la captura no se versiona y se documenta su motivo.
3. **Etiqueta de integridad**: todas las capturas quedan registradas como `operator-reported` — observación humana del run documentado; ni reproducibles desde Git ni evidencia de auditoría formal.
4. **Sin metadatos sensibles**: sin nombres de usuario reales, correos, tokens ni contenido de tablas raw/PII. Los valores mostrables son los agregados ya públicos del hito (640,00 / 5 / 0; 812,20 / 13 / 2; 15/10/7; 6).

## Checklist de captura (6 capturas)

Para cada una: abrir la ubicación indicada con sesión autenticada → capturar pantalla completa del panel → redactar según su lista → guardar con el nombre exacto en `docs/evidencia/evidence/screenshots/` → registrar su estado en [`evidence-index.yml`](evidence-index.yml).

### 1. `01-gold-celda-publicada.png` — Celda Gold publicada

- **Dónde:** Databricks → **Catalog** → catálogo sintético → schema `gold` → tabla `daily_sales_segment` → pestaña *Data*/*Preview* (o *Sample data*).
- **Resultado visible requerido:** exactamente **una fila física** con la celda paddle / ES-MD / web: `total_revenue_eur = 640.00`, `order_count = 5`, líneas reembolsadas `0`; **sin** fila de rubber.
- **Redactar:** URL del workspace, IDs de la tabla/volumen, rutas de almacenamiento en el panel de detalles, cualquier identidad humana.

### 2. `02-calidad-y-cuarentena.png` — Calidad y cuarentena agregadas

- **Dónde:** Catalog → schema `quarantine` → tabla `data_quality_rejections` (*Data*/*Preview*), y junto al reporte `silver.materialization_run_report` (schema `silver`) mostrando el estado de completitud.
- **Resultado visible requerido:** **6 filas de rechazo** con sus `rule_id` esperados (5 de orders, 1 de customers), sin payloads crudos visibles; reporte con estado **`COMPLETED_WITH_QUARANTINE`** y controles pre-supresión **812.20 / 13 / 2**.
- **Redactar:** URL del workspace, IDs, timestamps de sistema si exponen identidad, cualquier valor de contacto o identificador de cliente.

### 3. `03-configuracion-job-silver.png` — Configuración del job Silver

- **Dónde:** **Workflows** → Jobs → el job de materialización Silver `DEMO-001` → pestaña *Tasks* / *Details*.
- **Resultado visible requerido:** **dos tareas ordenadas** (`silver_materialization` → `silver_demo_001_verify`), sin schedule, `max_concurrent_runs: 1`, `max_retries: 0`, parámetros solo `batch_id`/`audit_timestamp`, y el run-as configurado como service principal.
- **Redactar:** **job ID completo**, URL del workspace, **application ID del SP** (mostrar solo "service principal dedicado"), correos/nombres de personas, rutas de workspace completas si exponen cuentas.

### 4. `04-dag-run-exitoso.png` — DAG del run exitoso

- **Dónde:** Workflows → el job → pestaña *Runs* → el run exitoso del hito → vista de grafo/DAG.
- **Resultado visible requerido:** las **dos tareas en verde** con su dependencia materialización → verificación, duración razonable y estado de éxito del run.
- **Redactar:** **run ID y task run IDs completos**, URL del workspace, timestamps exactos si se prefieren omitir, identidades.

### 5. `05-salida-verificador.png` — Salida del verificador read-only

- **Dónde:** Workflows → run exitoso → tarea `silver_demo_001_verify` → *Logs* / salida estándar.
- **Resultado visible requerido:** el JSON del verificador con **todos los chequeos `passed`** — recuentos Silver **15/10/7**, cuarentena **6**, celda Gold **640.00/5/0**, controles pre-supresión **812.20/13/2**, estado **`COMPLETED_WITH_QUARANTINE`**, ausencia de columnas PII en Gold.
- **Redactar:** URL del workspace, IDs de run/tarea, rutas de workspace, cualquier token del log.

### 6. `06-permisos-uc.png` — Permisos de Unity Catalog

- **Dónde:** Catalog → catálogo sintético → pestaña *Permissions* (y, si cabe en captura, los de los schemas `silver`/`bronze`).
- **Resultado visible requerido:** el **SP dedicado de Silver con solo sus grants mínimos** (`USE_CATALOG`; `USE_SCHEMA` en bronze; `SELECT` limitado a las tres tablas raw; `USE_SCHEMA`+`CREATE_TABLE` en silver/quarantine/gold) y el grupo escritor sin `CREATE_TABLE` en `bronze`.
- **Redactar:** **application IDs / nombres de SP y de grupos humanos** (dejar visibles solo los privilegios, no las identidades), URL del workspace.

## Estado general

- Estado de las 6 capturas: **`pending_operator_capture`** (ver [`evidence-index.yml`](evidence-index.yml)).
- Hasta que existan y estén redactadas, esta documentación se apoya en la evidencia textual versionada: [acta del hito Silver](../../fase-3/ACTA_EVIDENCIA_CLOUD_DEMO-001.md) y [borrador de cierre de Fase 2 (sin firmar)](../../fase-2/EVIDENCIA_CLOUD_DEMO-001_CIERRE_EXCEPCION_BORRADOR.md).
- Este plan no inventa `captured_at`, hashes ni archivos: cada campo se rellena solo cuando la captura existe.
