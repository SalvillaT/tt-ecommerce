# Fase 2 — Runbook de validación Bronze

**Estado:** Fase 2 **en implementación local; despliegue cloud pendiente.** Los procedimientos siguientes describen las pruebas y evidencias esperadas de la primera validación cloud de Bronze; ninguna se ha ejecutado todavía y este documento no autoriza su realización. Los comandos con parámetros concretos (cómo lanzar el job, consultas exactas) se completarán cuando la implementación genere sus artefactos; lo que sigue fija **qué se hace, qué se comprueba y qué evidencia queda registrada**, con las rutas, tablas y límites del [contrato canónico](ARQUITECTURA_Y_DECISIONES.md).

## Prerequisitos de cada sesión de validación

- [ ] Gates GF2-1 a GF2-9 de [Gates de despliegue](GATES_DESPLIEGUE.md) aprobados y, en particular, la autorización explícita de cómputo **para esta ejecución** (tipo de compute, tamaño, autoterminación) y la subida autorizada del lote `DEMO-001`; los flags de Fase 2 quedan irreversibles tras el apply y no se resetean.
- [ ] Modelo de acceso verificado: el productor/operador sube y promueve `_staging` exclusivamente; el job solo lee las **cuatro external locations de subruta** (`orders`, `customers`, `products`, `_control`) y se prueba su **imposibilidad de leer `_staging`** con evidencia; el rol del conector en el contenedor `landing` se documenta como Reader de alcance Azure-pancontenedor, no como aislamiento de identidad.
- [ ] Sesión trabajando **solo** con lotes sintéticos verificados contra manifest. Recuentos de referencia de `DEMO-001`: **26 líneas físicas de orders (25 parseables + 1 malformada), 11 filas de customers y 7 filas de products; 44 filas declaradas en total** por el manifest.
- [ ] Identidades acordadas: el operador (subida/promoción en `landing`) y el service principal del job (Auto Loader, volumen de estado, escritura Bronze); ninguna otra identidad interviene.
- [ ] SQL Warehouse desactivado y sin arrancar; ningún schedule del job activo (todo es ejecución manual visible).
- [ ] Preflight de posesión: las tres tablas raw y `bronze.ingestion_run_log` **no existen todavía** (ni Terraform ni un administrador las precrearon); el plan declaró solo `CREATE_TABLE` sobre `bronze` para el run-as del job, que será el único creador/propietario.
- [ ] Al terminar: comprobar compute y Warehouses según el [ciclo de Fase 1](../fase-1/CICLO_DE_DESARROLLO.md); registrar el coste estimado/observado de la sesión.

## P-0 — Preparación y promoción del lote

Promoción operatorial con la utilidad local `scripts/promote_batch.py` (Fase A del cierre F2): plan determinista sin Azure y publicación con Azure CLI (`az storage fs` con `--auth-mode login` y `--overwrite false`, sin SDK, secretos ni `shell=True`). La utilidad valida el directorio local contra el manifest, comprueba que ningún destino exista, crea los directorios necesarios y ejecuta el orden exacto manifest control → tres archivos finales → ready marker; tras cada upload descarga y recalcula el SHA-256.

1. Generar localmente el lote y revisar su manifest (`data/generated/DEMO-001/manifest_DEMO-001.json`; el fixture canónico está en [`data/sample/DEMO-001/`](../../data/sample/DEMO-001/)). El directorio local promocionado debe ser plano y contener **exactamente los cuatro archivos del contrato** (manifest + orders + customers + products; el fixture byte a byte idéntico está en `data/sample/DEMO-001`, que además lleva su `codemap.md` documental: para el comando se usa el lote regenerado en `data/generated/DEMO-001` o una copia plana equivalente del fixture).
2. **Plan offline (sin Azure)**, antes de cualquier subida; el JSON del plan es parte de la evidencia:

   ```powershell
   py scripts/promote_batch.py plan --batch-id DEMO-001 --arrival-date 2026-01-10 --local-batch-dir data/generated/DEMO-001 > promotion_plan_DEMO-001.json
   ```

   El plan muestra las rutas lógicas `landing/...` y sus equivalentes relativos al filesystem `landing`, los receipts (26/11/7, total 44) y el orden de pasos. Un plan con error (`--arrival-date` inválida, bytes que no casan con el manifest) termina con exit code distinto de cero y no produce plan.

3. **Autorización previa — obligatoria:** `publish` **solo se ejecuta cuando el gate GF2-6 ya fue aprobado y registrado**. La ejecución del comando nunca constituye una autorización implícita: sin gate aprobado, el plan se conserva y el trabajo queda local. La identidad ejecutora es la del operador autenticado previamente con `az login` (subida y promoción exclusivas del productor/operador; el job del pipeline jamás lee `_staging`).
4. **Publicación** (solo tras GF2-6), contra la Storage Account de Fase 1:

   ```powershell
   py scripts/promote_batch.py publish --batch-id DEMO-001 --arrival-date 2026-01-10 --local-batch-dir data/generated/DEMO-001 --account-name <storage_account_name>
   ```

   Cada paso emite un evento JSON con la operación completada; `--auth-mode login` y `--overwrite false` van fijados en todos los comandos `az storage fs` que la utilidad emite (directorios con `--name`, archivos con `--path`). Si **cualquier** paso falla (comando de Azure con exit code distinto de cero, SHA-256 remoto divergente tras descargarlo, cualquier error), la utilidad se detiene inmediatamente: no limpia, no sobrescribe, no reemite y **no inicia** una nueva promoción sin reconciliación. Los límites de garantía sobre el archivo ready son exactos:

   * Un fallo **antes** de intentar `publish-ready` nunca escribe ready (es la última operación y su guardia de colisión corre justo antes del intento): el estado remoto de ready tras ese fallo es simplemente *ausente*.
   * Un fallo **durante** `publish-ready` — el intento de subida o su verificación posterior (descarga + SHA-256) — eleva `PromotionReadyIndeterminateError`: el estado remoto de ready es **indeterminado** (puede existir con contenido canónico, existir con contenido divergente o no existir). Esto es así incluso cuando Azure CLI informa error: la operación pudo haber persistido el objeto a pesar del fallo del cliente. La utilidad no limpia ni sobrescribe en ningún caso; la **ingesta del lote queda bloqueada** hasta reconciliación humana con lectura remota directa, p. ej. `az storage fs file exists/download` sobre `_control/ready/DEMO-001.json`.

   El estado parcial de cualquier otro paso (staging, manifest de control, archivos finales) deja la siguiente ejecución del mismo `batch_id` en colisión (la utilidad la rechaza con `PromotionCollisionError`) y exige reconciliación humana; no se reintentan lotes con fallos sin esa reconciliación. Se evita intervenir manualmente el estado resultante; reenviar un lote rechazado por hash exige un `batch_id` nuevo o la solución aprobada en el acta.
5. **Verificación con evidencia:** la utilidad ya fuerza verificación descarga+SHA-256 tras cada upload (staging, manifest de control, tres finales y ready). Complementar con lecturas directas de solo lectura con la identidad del operador:

   ```powershell
   az storage fs file exists --account-name <storage_account_name> --file-system landing --path _control/ready/DEMO-001.json --auth-mode login
   ```

   Registrar en la evidencia: el plan JSON (`promotion_plan_DEMO-001.json`), los eventos JSON de cada paso publicados en orden (manifest de control antes que los tres finales; ready `landing/_control/ready/DEMO-001.json` **como última escritura** del lote, conteniendo `batch_id` y el SHA-256 del manifest) y el listado exacto de archivos promovidos (nombres, tamaños, orden). `_staging` queda intacto (`cleanSource` no entra aquí: la inmutabilidad es de la landing final, el staging se conserva hasta cierre). Los ficheros temporales de descarga/verificación usados por la utilidad viven en un directorio temporal exclusivo de la sesión y ninguna promoción sobreescribe rutas ajenas.

## P-1 — Primera ingesta

Procedimiento: capturar recuento de filas de las tres tablas raw y del log **antes** de ejecutar, lanzar el job una vez, esperar a que finalice y capturar el estado posterior.

Ejecución vía Declarative Automation Bundle (Fase B; solo tras los gates GF2-1..GF2-7 y su autorización; ningún comando despliega o ejecuta por sí solo):

```powershell
# Validación estática (sin Azure/Databricks remoto): puede repetirse offline.
databricks bundle validate -t development_serverless --var "run_as_service_principal_name=<sp-application-id>" --var "landing_root=<abfss://landing@<storage>.dfs.core.windows.net>"

# Despliegue del bundle: SOLO tras gates aprobados y autorización registrada.
databricks bundle deploy -t development_serverless --var "run_as_service_principal_name=<sp-application-id>" --var "landing_root=<abfss://landing@<storage>.dfs.core.windows.net>"

# Ejecución manual única (max_concurrent_runs=1, max_retries=0, sin schedule):
# cada lanzamiento requiere la aprobación de cómputo de GF2-7 para ESTA ejecución.
databricks bundle run bronze_synthetic_ingestion_job -- --batch_id DEMO-001 --arrival_date <YYYY-MM-DD> --landing_root <abfss://landing@<storage>.dfs.core.windows.net>
```

Target alternativo si serverless no está disponible: `development_classic` (mismo job, cómputo clásico con autoterminación, también sujeto a GF2-7). El script del job (`scripts/run_bronze_job.py`) exige `--batch-id`, `--arrival-date` y `--landing-root`; el preflight remoto de bytes corre antes de crear tablas/log y sin `_staging`.

**Semántica del run-log (importante para la evidencia):** `bronze.ingestion_run_log` es un log de **eventos** append-only por intento y fuente (`STARTED` y `MANIFEST_VALIDATED` por run sin fuente; `SOURCE_COMMITTED` por fuente terminada; `FAILED` solo ante un **error de una fuente tras el bootstrap** — los errores al crear tablas (DDL) o al escribir los eventos iniciales abortan con exit != 0 sin prometer ningún evento `FAILED`), **no una única fila final por fuente**. Reintentos añaden eventos nuevos. Las métricas por fuente son solo del progreso realmente disponible de la query terminada (`numInputRows` en `lastProgress`/`recentProgress`) y distinguen `metrics_observed`: `false` = progreso no disponible, `true` = `numInputRows` observado (incluido un 0 real). Filas escritas, rescatadas, malformadas, archivos o integridad **no se infieren** del manifest ni del total de la tabla: quedan en 0 si no son verificables por otra vía. Un run sin progreso (o con progreso 0) **no acredita por sí mismo** ningún resultado: la evidencia es la combinación de eventos del log, recuentos Bronze antes/después y pruebas de acceso.

Evidencia esperada:

- Job ejecutado una única vez con run-as del service principal; identificador de run de cada stream (3 fuentes).
- **Posesión verificada:** tras la primera ingesta, el propietario efectivo de `orders_raw`, `customers_raw`, `products_raw` y `bronze.ingestion_run_log` es únicamente el run-as del job (creadas por él, no precreadas por Terraform o admin), con inventario de grants heredados capturado junto a la evidencia.
- **Alcance de lectura firmado:** evidencia del job con acceso de lectura solo a las cuatro external locations de subruta y prueba del rechazo de lectura sobre `_staging` (la verificación negativa se ejecuta esta vez o se referencia la de los gates si es de la misma sesión autorizada).
- Filas en `bronze.ingestion_run_log` posteriores: **eventos** — `STARTED` y `MANIFEST_VALIDATED` por run (sin fuente) y `SOURCE_COMMITTED` por fuente con métricas de progreso (`numInputRows` si estuvo disponible, con `metrics_observed`). Un reintento añade eventos, no reemplaza filas; errores de DDL/eventos iniciales abortan sin evento `FAILED` garantizado.
- Recuento de filas raw **comparado con el manifiesto**: Bronze recibe líneas de datos equivalentes a las de los archivos, incluidos duplicados deliberados (`exact_duplicate`, `version_conflict`) y las filas de las anomalías; la resolución lógica es de la Fase 3 y su presencia aquí no es un fallo.
- Procedencia consultable: al menos una fila raw con `batch_id = 'DEMO-001'`, `_metadata` con ruta física del archivo origen (incluido su `arrival_date`/segmento de lote), tamaño y mtime, y timestamp de ingesta coherente.
- Contenido raw: las columnas físicas de la fuente están en texto tal cual llegaron, `parsed_payload_canonical` es contenido parseado/reconstruido **no byte-exacto** (los bytes exactos solo son acredtables por SHA-256 en la landing inmutable), y ninguna tipificación semántica ni regla de calidad se ha aplicado todavía (Fase 3).
- Checkpoint y ubicación de esquema existentes **solo** en `pipeline_state.ingestion_state` (`/Volumes/tt_ecommerce_synthetic/pipeline_state/ingestion_state/<source>/{schema,checkpoints/DEMO-001}`), con verificación explícita de cero artefactos de estado en `landing` (incluidos `_staging`/`_control`) o en el almacenamiento de tablas.
- La landing final permanece inmutable tras la ingesta (nada borrado, movido o renombrado).

## P-2 — Reproceso sin archivos nuevos (idempotencia)

Procedimiento: capturar por **consulta** los recuentos de filas de las tres tablas raw **antes** de la ejecución; sin añadir, mover ni renombrar archivos, relanzar el mismo job una segunda vez; repetir las mismas consultas **después**.

Evidencia esperada:

- **Recuentos de Bronze: la evidencia primaria.** Los recuentos de `bronze.orders_raw`, `bronze.customers_raw` y `bronze.products_raw` capturados antes y después de la segunda ejecución son **idénticos** (cero incremento de filas raw); ambos valores se capturan y se registran.
- El run-log es un log de eventos: para este segundo intento añade los eventos nuevos (`STARTED`, `MANIFEST_VALIDATED` y `SOURCE_COMMITTED` por fuente); no se exigen métricas de archivos o filas escritas porque el run-log no las mide.
- Métricas honestas por fuente: si la query expuso `numInputRows`, el evento `SOURCE_COMMITTED` lo registra con `metrics_observed = true` (un 0 observado real es distinto de progreso ausente, que se registra con `metrics_observed = false`); el run-log **no infiere** filas escritas, rescatadas, malformadas, archivos ni integridad, y un precio de 0 filas o progreso ausente **no acredita por sí solo** el resultado: lo acreditan los recuentos antes/después más los eventos.
- Al ser append-only por intento, los reintentos añaden eventos sin reemplazar filas: el log crece, las tablas no; es el comportamiento esperado, no un fallo (no hay exactly-once atómico entre tablas).
- La landing final sigue inmutable y los checkpoints retenidos no retroceden.

## P-3 — Recuperación desde checkpoint

Procedimiento: interrumpir la ejecución de forma controlada (cancelar el job mid-run en una sesión autorizada para ello) y relanzar después, sin tocar el checkpoint.

Evidencia esperada:

- Antes de la interrupción y del relanzamiento: verificar por evidencia que el checkpoint retenido de la fuente en `pipeline_state.ingestion_state/checkpoints/<batch>` existe, es el original y no fue borrado, recreado ni reemplazado. Si no se puede acreditar esa condición, la prueba **queda anulada**: no se afirma recuperación con checkpoints perdidos/sustituidos.
- Tras el relanzamiento (verificado con **consultas a Bronze antes/después**): la ingesta continúa desde donde el checkpoint quedó; los archivos ya confirmados no se reingieren (recuentos de `bronze.<fuente>_raw` idénticos para lo ya confirmado) y los pendientes se procesan (recuento final por consulta Bronze, no desde el manifest ni del run-log).
- Los reintentos añaden **eventos** nuevos en `bronze.ingestion_run_log` (por fuente y run); esto se registra como esperado (la semántica de checkpoint del sink cubre las tablas raw; el log es un log de eventos, sin exactly-once). Sus métricas son solo el `numInputRows` realmente observado con su `metrics_observed`; nada más se infiere.
- No se usan `destroy`, reseteo, recreación ni borrado de checkpoints o tablas como método de corrección; cualquier acción manual queda prohibida y, si ocurre, invalida la prueba y se registra como excepción.

## P-4 — Drift de esquema (campo inesperado)

Procedimiento: tras la ingesta de `DEMO-001` (o con un lote preparado para el caso), consultar las filas del escenario `extra_json_field` (`ORD-EXTRA-01`, campo `source_campaign`) **con una consulta a Bronze**.

Evidencia esperada:

- La fila del escenario existe en `bronze.orders_raw` (`batch_id = 'DEMO-001'`) y su columna `_rescued_data` contiene `source_campaign` y su valor (`SYN-CAMP-01`); el esquema declarado de la tabla no cambió. La consulta Bronze (no el run-log) es la evidencia primaria de la anomalía.
- El run-log de esa ejecución **no está obligado a medir** filas rescatadas: las métricas disponibles son solo `numInputRows` con su `metrics_observed`; `rows_rescued` no se infiere desde el manifest ni desde aquí. Si se quiere evidencia de rescate vía métricas, debe venir de otra fuente aprobada en la revisión, no del run-log en Fase 2.
- El dato no se pierde silenciosamente ni genera un fallo fatal de la ejecución; el resto de las filas del archivo se ingiere normalmente (recuento por consulta Bronze: 25 líneas parseables, incluida la fila rescatada por drift, y 1 línea malformada en `orders` para `DEMO-001`).
- No se añadió el campo nuevo ni a la tabla ni a la lógica: el rescate es la solución explícita de Fase 2.

## P-5 — Registro malformado (JSONL cortada)

Procedimiento: consultar las filas de orders asociadas al escenario `malformed_json` (`ORD-MALF-01`, última de las 26 líneas físicas del JSONL, aislada por construcción) **con una consulta a Bronze**.

Evidencia esperada:

- La ejecución no aborta; las otras 25 líneas parseables del archivo se ingieren con sus datos íntegros (recuentos por consulta Bronze).
- El registro malformado es visible y consultable vía la columna `_corrupt_record`, que conserva la evidencia de la fila defectuosa, o por el mecanismo equivalente aprobado en la revisión de la implementación; en cualquier caso, existe evidencia de fila y no un descarte silencioso. La **consulta Bronze** es la evidencia primaria; el run-log no está obligado a medir filas corruptas en Fase 2.
- La ejecución queda marcada con el estado acordado en su secuencia de eventos del run-log `SOURCE_COMMITTED` (la anomalía no es fallo de la fuente), no como fallo sin diagnóstico; sus métricas son solo `numInputRows`/`metrics_observed` y no acreditan el conteo de malformados.
- Se documenta cómo diagnosticar `ORD-MALF-01` desde Bronze sin tocar el payload ni la landing inmutable, donde los bytes exactos siguen acredtables por el SHA-256 del manifest.

## Registro de evidencia

Cada prueba se registra por separado con, como mínimo: fecha e identidad ejecutora; autorizaciones que amparan la ejecución (gate + aprobación de cómputo de GF2-7); identificadores de run; consultas y recuentos capturados (antes/después); filas de `bronze.ingestion_run_log` correspondientes; referencias de rutas de estado (`pipeline_state.ingestion_state`) y de landing verificadas; capturas de las verificaciones positivas y negativas de acceso; y el estado del coste al cierre. La evidencia es el reporte del usuario y se consolidará en la documentación con esa condición declarada; este repositorio por sí solo no constituye evidencia cloud directa.

## Prohibido durante la validación

- Ejecutar cualquier ingesta sin gate aprobado ni aprobación de cómputo para esa ejecución.
- Que el job o el pipeline lean `_staging`, o conceder `READ_FILES` sobre él: la subida y la promoción son exclusivas del productor/operador.
- Precrear tablas o log con Terraform, portal o admin antes de la primera ingesta; conceder `SELECT`/`MODIFY` adicionales más allá del `CREATE_TABLE` inicial del job sobre `bronze`.
- Añadir o promover lotes no autorizados; solo `DEMO-001` (u otro lote aprobado explícitamente) puede llegar a la landing final.
- Mutar, borrar o truncar tablas Bronze, el log de ejecución, el manifest de control, el archivo ready o la landing final para "arreglar" resultados.
- Borrar, recrear o reemplazar checkpoints y reclamar recuperación desde un estado no intacto: si el checkpoint no está retenido e intacto, la recuperación no se afirma y el reproceso se documenta como ingesta nueva.
- Arrancar o crear el SQL Warehouse; escribir Silver, cuarentena o Gold; aplicar tipado semántico o reglas de calidad a las columnas raw (son de Fase 3).
- Usar `terraform destroy` o `-target`, o resetear los flags de Stage 2 o de habilitación/aprobación de Fase 2 tras su apply (son operacionalmente irreversibles; su retirada exige un cambio revisado por separado).
- Registrar en Git IDs, secretos, tokens, `tfstate` o contenido de datos.
