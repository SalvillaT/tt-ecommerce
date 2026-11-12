# Fase 2 — Plantilla de evidencia cloud (GF2-0…GF2-10 y P-0…P-5)

**Estado:** PLANTILLA SIN RELLENAR. Ningún registro de esta página constituye evidencia ni aprobación: es el formato acordado que el operador rellenará **por sesión autorizada** durante la validación cloud de Fase 2, siguiendo [gates](GATES_DESPLIEGUE.md) y [runbook](RUNBOOK_VALIDACION.md). Cada bloque se copia en una bitácora de sesión separada antes de rellenarse; nunca se edita esta plantilla marcando un gate como aprobado — el relleno vive en la copia de sesión, firmada por el responsable.

**Reglas de la plantilla:**

- Un bloque solo se da por firmado cuando **todos** sus campos obligatorios («campos mínimos») están rellenados y la **Aprobación** está registrada por el rol que ampara el gate o la prueba. Un campo pendiente deja el bloque en estado `SIN FIRMAR` y cualquier ejecución posterior condicionada a ese gate **no procede**.
- No se registran secretos, tokens, cadenas de conexión ni identificadores sensibles: los valores redactables se sustituyen por placeholders (`<storage_account>`, `<sp-application-id>`, `<tenant>`). Ningún nombre real de tenant, suscripción ni cuenta de almacenamiento entra en Git.
- Los hashes de comandos y plans se calculan sobre el artefacto redactado (el plan o el comando con placeholders), no sobre versiones con valores reales.
- La evidencia aquí es el **reporte del operador** consolidado en la documentación; este repositorio por sí solo no constituye evidencia cloud directa.

## Campos mínimos de cada registro

| Campo | Contenido esperado |
| --- | --- |
| Responsable | Rol e identidad ejecutora (con placeholders si procede): operador, productor, run-as del job, aprobador. |
| Timestamp (UTC) | Fecha/hora de inicio y de cierre del gate o de la prueba. |
| Comando / hash del plan redactado | Comando exacto ejecutado (con placeholders) y SHA-256 o enlace al resumen del artefacto redactado (p. ej. `terraform plan` JSON redactado, `promotion_plan_DEMO-001.json`). |
| Permisos / grants | Grants y roles verificados en este paso: tocados, positivos y negativos (con placeholders en identificadores), incluidos inventarios heredados cuando aplique. |
| Resultado | Resultado factual observado (salidas, recuentos, estados), sin rellenar lo no verificado. |
| Coste | Registro de coste aplicable (aprobación previa de cómputo, coste observado al cierre, alerta). |
| Enlace / evidencia | Ruta o referencia al artefacto guardado (captura, JSON de eventos, salida de consulta, plan). |
| Aprobación | Quién aprueba y con qué autoridad; fecha; y qué habilita el siguiente paso. |

---

## GF2 — Gates de despliegue (bloques por copiar)

### GF2-0 — Alcance (solo sintéticos, condiciones G5)

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | |
| Permisos / grants | — (n/a) |
| Resultado | Confirmación registrada: exclusivamente lotes sintéticos con manifest, primer lote `DEMO-001`; G5 vigente (endpoints públicos sin allowlist, principals autenticados, datos sintéticos; sin PII ni datos reales). |
| Coste | |
| Enlace / evidencia | |
| Aprobación | |

### GF2-1 — Identidad del pipeline (run-as dedicado)

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | |
| Permisos / grants | SP dedicado `<sp-application-id>` creado/asignado como run-as; sin roles admin ni acceso general a suscripción; separado del operador, del principal Terraform y de los conectores existentes. |
| Resultado | |
| Coste | |
| Enlace / evidencia | |
| Aprobación | |

### GF2-2 — Acceso a landing (conector Reader + cuatro external locations)

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | |
| Permisos / grants | Conector nuevo con rol Reader en el contenedor `landing`; credencial UC propia; cuatro external locations de subruta (`orders`, `customers`, `products`, `_control`) con `READ_FILES` solo para el run-as. **Positivos:** lectura del job en las cuatro locations. **Negativos:** job sin acceso a `_staging`; job sin acceso fuera de `landing` (contenedor de managed storage y `lakehouse`); identidad no autorizada sin acceso. Sin tocar el conector/credencial/location de `lakehouse`. |
| Resultado | |
| Coste | |
| Enlace / evidencia | |
| Aprobación | |

### GF2-3 — Estado del pipeline (volumen UC `pipeline_state.ingestion_state`)

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | |
| Permisos / grants | Volumen creado con grants mínimos solo para el run-as; único creador/propietario del estado. |
| Resultado | Rutas canónicas verificadas tras la primera ingesta: `/Volumes/tt_ecommerce_synthetic/pipeline_state/ingestion_state/<source>/{schema,checkpoints/<batch>}`; **cero artefactos de estado** en `landing` (incluidos `_staging`/`_control`) ni en el almacenamiento de tablas; checkpoints retenidos. |
| Coste | |
| Enlace / evidencia | |
| Aprobación | |

### GF2-4 — Posesión y privilegios mínimos de Bronze

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | |
| Permisos / grants | Terraform declara solo `CREATE_TABLE` sobre `bronze` para el run-as (`databricks_grant.phase2_job_bronze_schema`); grupo escritor con `CREATE_TABLE` solo en `silver`/`quarantine`/`gold`; sin `SELECT`/`MODIFY` extra; inventario de grants heredados registrado. |
| Resultado | **Inexistencia de tablas antes del job:** consulta UC que acredita que `orders_raw`, `customers_raw`, `products_raw` y `bronze.ingestion_run_log` no existen antes de la primera ejecución. Tras la primera ingesta: propietario efectivo de las cuatro tablas = únicamente el run-as. |
| Coste | |
| Enlace / evidencia | |
| Aprobación | |

### GF2-5 — IaC en tres planes/aplicaciones secuenciados

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | (1) layout → (2) conector Reader + Azure RBAC y credencial → (3) fundación UC. Cada plan con su **hash de plan redactado** y la secuencia respetada; sin `destroy` ni `-target`; flags Stage 2 en `true`. |

> Nota factual del cierre local: el `terraform validate` de `infra/databricks/` no puede ejecutarse en el host de preparación (Application Control bloquea el arranque del provider); la validación y los planes de este root corresponden al host autorizado del operador y **ningún plan se firma aquí**. `infra/bootstrap/` e `infra/platform/` pasaron `validate` en el host de preparación (Terraform 1.16.6).

| Campo | Registro |
| --- | --- |
| Permisos / grants | Por plan: inventario previo de locations/grants/propietarios solapados; grants tocados por el plan; verificación post-apply. |
| Resultado | **Tres planes secuenciales sin destrucciones inesperadas:** cada plan revisado muestra 0 destrucciones y 0 reemplazos de objetos de Fase 1/Stage 2 y ninguna destrucción inesperada; aplica secuenciales con autorización separada por plan; ante fallo parcial: estado alcanzado consignado sin `destroy`/`-target`. |
| Coste | |
| Enlace / evidencia | |
| Aprobación | (una por plan) |

### GF2-6 — Subida autorizada del lote (promoción)

| Campo | Registro |
| --- | --- |
| Responsable | Productor/operador (única identidad autorizada). |
| Timestamp (UTC) | |
| Comando / hash | `py scripts/promote_batch.py plan --batch-id DEMO-001 --arrival-date <YYYY-MM-DD> --local-batch-dir data/generated/DEMO-001` (plan JSON como evidencia) y, tras la aprobación, `publish --account-name <storage_account_name>`. |
| Permisos / grants | Identidad operador con su rol de datos sobre `landing`; el job no interviene. |
| Resultado | Verificación SHA-256/recuentos contra manifest antes y después; manifest de control en `landing/_control/manifests/DEMO-001/` y archivo ready `landing/_control/ready/DEMO-001.json` como última escritura; `_staging` intacto; directorios finales con exactamente los archivos del contrato. |
| Coste | |
| Enlace / evidencia | |
| Aprobación | |

### GF2-7 — Cómputo efímero y por ejecución

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | Target elegido (`development_serverless` / `development_classic`); `bundle run` por ejecución con `--batch_id`/`--arrival_date`. |
| Permisos / grants | — (n/a) |
| Resultado | Aprobación de coste/configuración **por esta ejecución** (tipo, tamaño, autoterminación); `timeout_seconds: 3600` del run; SQL Warehouse desactivado; al cierre, cero compute activo. |
| Coste | Aprobación previa + coste observado al cierre. |
| Enlace / evidencia | |
| Aprobación | **Por ejecución**: una aprobación firmada por cada `bundle run`. |

### GF2-8 — Revisión humana final

| Campo | Registro |
| --- | --- |
| Responsable | Revisor/revisores humanos. |
| Timestamp (UTC) | |
| Comando / hash | — (n/a) |
| Permisos / grants | Revisión conjunta de RBAC/grants, rutas, nombres, manifest de auditoría. |
| Resultado | Revisión de coste, plan de rollback y destinatarios de alertas; veredicto. |
| Coste | |
| Enlace / evidencia | |
| Aprobación | |

### GF2-9 — Irreversibilidad operativa de flags

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | |
| Permisos / grants | — (n/a) |
| Resultado | Flags de Fase 2 declarados operacionalmente irreversibles tras el apply; ningún plan los resetea; retirada solo por cambio revisado y autorizado por separado. |
| Coste | |
| Enlace / evidencia | |
| Aprobación | |

### GF2-10 — Job, bundle y concurrencia

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | `databricks bundle validate -t <target> --var "run_as_service_principal_name=<sp-application-id>" --var "landing_root=abfss://landing@<storage_account>.dfs.core.windows.net"` (estático, sin vars reales) y, tras aprobación, `deploy`; `run` solo con `--batch_id`/`--arrival_date`. |
| Permisos / grants | Run-as = SP de GF2-1; job creado sin schedule, con `max_concurrent_runs=1`, `max_retries=0`, `timeout_seconds=3600`; `landing_root` fijado como configuración del bundle **antes del deploy** (raíz exacta del contenedor; argumento fijo de la tarea, no parámetro del run). |
| Resultado | Job registrado en el workspace con la configuración declarada; serverless o clásico según disponibilidad/GF2-7. |
| Coste | |
| Enlace / evidencia | |
| Aprobación | |

---

## RE-PA — Requisitos de evidencia post-apply/pre-ingesta (no gates)

Los dos requisitos siguientes **no son gates aprobados localmente** y **ningún checkbox GF2 los marca**: se ejecutan en el runtime cloud autorizado, **tras los applies de GF2-5 y antes de la primera ingesta**, y se firman aquí con los mismos campos mínimos. Complementan los gates y las pruebas P-0…P-5 y se copian por sesión como el resto de bloques; sin ellos firmados, la validación no queda acreditada.

### RE-PA-1 — Auditoría negativa en runtime: rechazo de subdirectorio con nombre aprobado

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | Ejecución **en el runtime autorizado** de la auditoría metadata-only del directorio de lote con **lookup recursivo**, contra un directorio de control (creado y retirado con autorización expresa de sesión y ciclo consignado) que contiene un **subdirectorio con un archivo con el mismo nombre exacto aprobado**. |
| Permisos / grants | Identidad que ejecuta la auditoría/constatación (operador autorizado; el job no interviene en la prueba, salvo documentación expresa de lo contrario en la sesión). |
| Resultado | **Rechazo observado en runtime** del subdirectorio con nombre exacto aprobado, antes de leer bytes, de cualquier DDL o de cualquier stream; sin lectura de contenido (listado metadata-only). |
| Coste | Cero compute si es operación metadata-only de almacenamiento; se consigna. |
| Enlace / evidencia | |
| Aprobación | |

### RE-PA-2 — Inmutabilidad operativa/único escritor entre preflight y `availableNow`

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | Listado metadata-only inicial de los directorios finales en el preflight; referencia del `bundle run` y de la conclusión del trigger `availableNow`; constatación final. |
| Permisos / grants | — (n/a) |
| Resultado | **Cero escrituras ajenas** en los directorios finales entre el preflight y la conclusión de `availableNow` (único escritor: productor/operador en la ventana de promoción; el job solo lee). Se consigna explícitamente que **no se promete atomicidad SHA→lectura**: el SHA-256 verificado en la promoción no acredita por sí solo los bytes que el job lee; la garantía de la ventana es **operativa (único escritor evidenciado), no estructural**. Cualquier escritura ajena en la ventana invalida la prueba en curso, documentada sin mutar Bronze. |
| Coste | — (n/a) |
| Enlace / evidencia | |
| Aprobación | |

---

## P-0…P-5 — Pruebas del runbook (bloques por copiar)

Condiciones que cada bitácora de sesión acreditará antes de declarar la validación:

1. Los **tres planes Terraform** de GF2-5 se ejecutaron en secuencia, sin destrucciones inesperadas, con autorización separada por plan.
2. Los **permisos** quedaron probados con resultados **positivos y negativos** (cuatro locations para el job; `_staging`, fuera de `landing` y no autorizados denegados).
3. Las tablas Bronze y el log **no existían** antes de la primera ejecución del job.
4. La prueba usa `DEMO-001` con recuentos por fuente **26/11/7** (26 líneas físicas de orders —25 parseables + 1 malformada—, 11 customers, 7 products; 44 declaradas por el manifest).
5. El **owner run-as**: tras la primera ingesta, propietario efectivo de las cuatro tablas = el SP del job.
6. El **run-log** `bronze.ingestion_run_log` queda capturado como registro de eventos (no métricas inferidas).
7. El **checkpoint** quedó retenido e intacto en `pipeline_state.ingestion_state`, físicamente aislado de `landing` y de las tablas.
8. `corrupt` (`ORD-MALF-01` en `_corrupt_record`) y `rescued` (`ORD-EXTRA-01` en `_rescued_data`) visibles por consulta Bronze.
9. El **segundo run** (P-2) se ejecutó **con el checkpoint intacto** y recuentos Bronze antes/después idénticos.
10. El requisito **RE-PA-1** quedó firmado: tras los applies y antes de la primera ingesta, la auditoría metadata-only con lookup recursivo ejecutada **en el runtime autorizado** rechazó un subdirectorio que contiene un archivo con el nombre exacto aprobado (requisito de evidencia, no gate aprobado localmente).
11. El requisito **RE-PA-2** quedó firmado: el tramo **preflight → `availableNow`** evidenció **único escritor** de las directorios finales (listado inicial, cero escrituras ajenas, constatación final), consignando que **no se promete atomicidad SHA→lectura** (el SHA de la promoción no acredita por sí solo los bytes leídos por el job).

### P-0 — Preparación y promoción del lote

| Campo | Registro |
| --- | --- |
| Responsable | Productor/operador. |
| Timestamp (UTC) | |
| Comando / hash | Plan JSON generado + eventos JSON publicados en orden (manifest de control → tres finales → ready última escritura). |
| Permisos / grants | — (n/a) |
| Resultado | Directorio local plano con exactamente los cuatro archivos; verificación SHA-256 tras cada upload; **inmutabilidad/solo escritor** del directorio final a partir de la promoción: ninguna escritura extra hasta el cierre de la sesión. El tramo **preflight → `availableNow`** se firma además en RE-PA-2 (único escritor, sin promesa de atomicidad SHA→lectura). |
| Coste | Almacenamiento nuevo, si se mide. |
| Enlace / evidencia | `promotion_plan_DEMO-001.json`, eventos de publicación, listado de archivos promovidos. |
| Aprobación | Condicionado a GF2-6 firmado; sin enlace a la ejecución del job si falla. |

### P-1 — Primera ingesta

| Campo | Registro |
| --- | --- |
| Responsable | Operador (lanzamiento); run-as (ejecución). |
| Timestamp (UTC) | |
| Comando / hash | Recuentos pre; `databricks bundle run bronze_synthetic_ingestion_job -- --batch_id DEMO-001 --arrival_date <YYYY-MM-DD>`; recuentos post. |
| Permisos / grants | Positivos y negativos (GF2-2) referenciados o re-medidos en esta sesión; propietario efectivo de tablas = run-as (GF2-4). |
| Resultado | Una única ejecución; eventos del log (`STARTED`, `MANIFEST_VALIDATED`, `SOURCE_COMMITTED` ×3) con `numInputRows`/`metrics_observed`; recuento Bronze comparado con el manifest **26/11/7 (44)**, incluidos duplicados deliberados; procedencia (`batch_id`, `_metadata` con ruta física/tamaño/mtime); checkpoint y esquema **solo** en `pipeline_state.ingestion_state`; landing final inmutable; ventana preflight → `availableNow` con cero escrituras ajenas evidenciada (RE-PA-2, único escritor; sin promesa de atomicidad SHA→lectura). |
| Coste | Aprobación GF2-7 de esta ejecución + coste observado. |
| Enlace / evidencia | Capturas de consultas y salidas del run. |
| Aprobación | |

### P-2 — Reproceso sin archivos nuevos (idempotencia, checkpoint intacto)

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | Recuentos pre/post por consulta; segundo `bundle run` del mismo lote; verificación previa de checkpoint intacto. |
| Permisos / grants | — (n/a) |
| Resultado | **Recuentos Bronze idénticos antes/después** (cero incremento raw); log con eventos nuevos del segundo intento; `numInputRows`/`metrics_observed` honestos (0 real ≠ ausencia); sin borrado/sustitución de checkpoints. |
| Coste | Aprobación GF2-7 de esta ejecución + coste observado. |
| Enlace / evidencia | |
| Aprobación | |

### P-3 — Recuperación desde checkpoint retenido

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | Interrumpir (cancel mid-run en sesión autorizada) → relanzar; verificación previa y posterior de checkpoint original. |
| Permisos / grants | — (n/a) |
| Resultado | Checkpoint intacto acreditado; continuidad desde donde quedó; ya confirmados no reingieren (recuentos por consulta); eventos nuevos en el log; sin `destroy`/reset/borrado de checkpoints (si ocurriera, la prueba queda anulada y documentada como excepción). |
| Coste | Aprobación GF2-7 de esta ejecución + coste observado. |
| Enlace / evidencia | |
| Aprobación | |

### P-4 — Drift de esquema (`ORD-EXTRA-01`)

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | Consulta Bronze sobre `bronze.orders_raw` con `batch_id = 'DEMO-001'`. |
| Permisos / grants | — (n/a) |
| Resultado | Fila presente con `_rescued_data` conteniendo `source_campaign` = `SYN-CAMP-01`; esquema declarado sin cambio; ejecución no aborta; 25 parseables + 1 malformada en orders. |
| Coste | |
| Enlace / evidencia | |
| Aprobación | |

### P-5 — Registro malformado (`ORD-MALF-01`)

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | Consulta Bronze de la última línea física del JSONL. |
| Permisos / grants | — (n/a) |
| Resultado | Evidencia consultable vía `_corrupt_record` (o mecanismo equivalente aprobado); sin descarte silencioso; `SOURCE_COMMITTED` de la fuente no la marca como fallo; bytes exactos acreditados en la landing inmutable por SHA-256. |
| Coste | |
| Enlace / evidencia | |
| Aprobación | |

---

## Cierre de la validación cloud

| Campo | Registro |
| --- | --- |
| Responsable | |
| Timestamp (UTC) | |
| Comando / hash | — |
| Permisos / grants | Estado final de grants tras la sesión (sin `SELECT`/`MODIFY` adicionales; sin cambios para terceros). |
| Resultado | Cobertura de gates GF2, de los requisitos de evidencia RE-PA-1/RE-PA-2 y de P-0..P-5 firmados; condiciones 1–11 arriba acreditadas; compute detenido; SQL Warehouse desactivado. |
| Coste | Coste acumulado de la sesión y alarma/presupuesto. |
| Enlace / evidencia | Bitácora completa de la sesión. |
| Aprobación | Cierre de Fase 2 cloud validada (solo si todos los bloques previos están firmados; cualquier hueco deja la fase `SIN FIRMAR`). |
