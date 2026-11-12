# Plan de desarrollo — TT E-commerce Lakehouse

## Propósito y forma de trabajo

Este plan convierte el diseño del README en un MVP reproducible, sin dar por existentes recursos ni permisos. Cada fase se cierra con evidencia revisable antes de iniciar la siguiente. Los datos de desarrollo son sintéticos; cualquier cambio de alcance que introduzca datos reales requiere una revisión específica de seguridad y retención.

**Responsabilidades previstas**

| Rol | Responsabilidad |
| --- | --- |
| Ingeniería de datos | Pipelines, contratos, calidad, modelo Gold y documentación operativa. |
| Plataforma/cloud | Suscripción, red/identidad, backend de estado, despliegue y límites de coste. |
| Administrador Databricks/Unity Catalog | Metastore, storage credentials/external locations según patrón aprobado, grupos, privilegios y políticas de columnas. |
| Responsable de negocio/BI | Definición y validación de métricas, umbrales de publicación y dashboard. |
| Seguridad/privacidad (si existe) | Revisión de grupos, secretos, retención y controles de salida. |

## MVP: fases dependientes

### Fase 0 — Diseño, contratos y datos sintéticos

**Objetivo:** fijar el comportamiento antes de provisionar o transformar datos.

**Estado: CERRADA (2026-11-02).** Especificación v1: [contratos y acta](fase-0/CONTRATOS.md#acta-de-decisiones-f0-v1), [métricas y calidad](fase-0/METRICAS_Y_CALIDAD.md), [linaje y privacidad](fase-0/LINAJE_Y_PRIVACIDAD.md).

**Dependencias:** ninguna.

**Tareas unitarias**

1. **Completado:** definir para `orders`, `customers` y `products` propietario, clave, tipos, campos obligatorios, semántica de estados, frecuencia y SLA objetivo. El grano acordado es línea de pedido (`order_id` + `line_id`).
2. **Completado:** documentar linaje desde cada archivo sintético a Bronze, Silver, cuarentena y métrica Gold.
3. **Completado:** crear un generador determinista con semilla y manifiesto de lote: pedidos JSON, clientes CSV y catálogo CSV; incluir duplicados, correcciones, cancelaciones/reembolsos, nulos, fechas tardías y un campo inesperado.
4. **Completado:** definir reglas de calidad: unicidad de línea de pedido (`order_id` + `line_id`), integridad referencial, rango de importe/cantidad, fechas válidas, completitud y regla explícita por cada nulo permitido o rechazado.
5. **Completado:** acordar ingresos netos según versión/estado vigente, pedido contado, fecha de negocio `Europe/Madrid` y reproceso de 7 días más fechas afectadas.
6. **Completado:** acordar publicación Gold únicamente para celdas canónicas con al menos 5 `customer_id` distintos y sin roll-ups solapados; no se asume que el criterio elimine el riesgo de reidentificación.

**Entregables:** contratos versionados, diccionario de datos, especificación del generador y matriz de calidad/linaje.

**Aceptación verificable:** una persona distinta puede regenerar el mismo lote desde la semilla; cada campo tiene tipo, nulabilidad y propietario; las métricas Gold tienen definición y ventana; se documenta el destino de cada fila inválida.

**Cierre y evidencia:** contratos, diccionario, reglas, linaje, generador, fixture `DEMO-001` (semilla 42, checksums en manifiesto) y CLI disponibles. La ejecución `py -m unittest discover -s tests -v` registró **42 tests** correctos. Se cumplen los criterios de aceptación de Fase 0 como especificación y evidencia local sintética; no implica jobs, tablas Gold, controles Unity Catalog ni recursos Azure desplegados. Fase 1 queda pendiente.

---

### Fase 1 — Fundaciones Azure, Terraform e identidad

**Objetivo:** preparar y verificar, tras despliegue autorizado, la fundación Azure y el control base de Unity Catalog antes de que Bronze/Silver reciban datos.

**Estado: CERRADA (según evidencia reportada por el usuario).** Región prevista: **France Central** para `platform` y `bootstrap` opcional. G5 de red está registrado: endpoints públicos sin allowlist para principals autenticados y datos sintéticos; no es red privada ni evidencia cloud directa. [Arquitectura y decisiones](fase-1/ARQUITECTURA_Y_DECISIONES.md) · [gates previos al despliegue](fase-1/GATES_DESPLIEGUE.md) · [guía de despliegue guiado](fase-1/GUIA_DESPLIEGUE_GUIADO.md). La evidencia reportada acredita lecturas directas de objetos UC/grants y una prueba con escritor no administrador: lectura de catálogo/esquemas permitida y acceso a external location/credencial denegado. Tras retirar el Global Administrator temporal de la administración cloud/UC, el perfil administrativo de Databricks siguió funcionando y el plan final de `databricks` convergió con 0 altas, 0 cambios y 0 destrucciones. El usuario reporta presupuesto mensual de suscripción de 50 EUR con alertas creado. Inventario reportado: 0 clusters, 0 jobs y un único SQL Warehouse preexistente detenido; Terraform conserva el Warehouse desactivado y no lo creó. No hay datos sintéticos, jobs, compute ni SQL Warehouse iniciado por el proyecto. Las Fases 2–5 siguen siendo trabajo separado; ningún plan debe mutar o reemplazar el metastore y no se permiten cleanup, `destroy` ni `-target`.

**Ciclo operativo aprobado:** conservar workspace y ADLS; detener Jobs/SQL entre sesiones sin `terraform destroy` rutinario. Véase [ciclo de desarrollo](fase-1/CICLO_DE_DESARROLLO.md).

**Dependencias:** Fase 0; acceso administrativo aprobado a Azure y Databricks; G5 de red registrado y gates de coste, identidad, backend y UC pendientes/aprobados según evidencia.

**Tareas unitarias**

1. Confirmar France Central, suscripción, convención de nombres, etiquetas, presupuesto/alertas y responsables; reevaluar coste persistente de ADLS/workspace y elegibilidad/cuota/Premium en esa región. La advertencia observada en West Europe no garantiza disponibilidad en France Central.
2. Diseñar módulos Terraform para resource group, ADLS Gen2, configuraciones de acceso necesarias, Databricks y dependencias soportadas por el provider elegido. El operador Azure CLI actual recibe por IaC Blob Data Contributor solo en `landing`/`lakehouse` para rutas; el Access Connector mantiene su RBAC independiente solo en `lakehouse`. Rutas en segunda etapa gated.
3. Configurar backend remoto protegido para `tfstate`: Storage Account separado, contenedor privado, endpoint público sin allowlist solo para Entra/RBAC autenticado, Shared Key/usuarios locales deshabilitados, OAuth por defecto y versionado/soft delete de 7 días. Los recursos y el role assignment de contenedor están reportados como aplicados, bootstrap conserva estado local y el usuario reporta backend remoto configurado para `platform`. Owner de suscripción no concede por sí solo acceso Entra al plano de datos Blob; un cambio posterior a SP/managed identity cambia el principal y exige revisar el plan.
4. El grupo único de escritores y su membresía ya existen según reporte, con Workspace access exclusivamente. Antes del apply, confirmar su nombre no versionado, membresía efectiva y que no recibió consumer/SQL/admin, clusters sin restricciones ni pools. No conceder acceso a analistas, BI, stewardship o `account users`; la matriz ampliada, vistas y máscaras se implementan en Fase 4.
5. Preparar identidades administradas/service principals y permisos mínimos para despliegue, jobs y acceso a almacenamiento, separando identidad humana y automatización.
6. Decidir y configurar Key Vault o el gestor corporativo de secretos si el patrón necesita secretos; comprobar que no se imprimen en logs ni se incluyen en variables versionadas.
7. El usuario reporta un metastore France Central asignado al workspace cuyo root DEFAULT permanece vacío. Validar, tras diseño y autorización separados, el parent external location y el child path exclusivo de managed storage a nivel de catálogo, sin solaparlo con `lakehouse`, que queda reservado para external location. Terraform no crea ni asigna el metastore; no se aplica el root hasta que identidad aplicadora y autoridad de objeto estén aprobadas.
8. Confirmar soporte/permisos del provider Databricks 1.80.0 y, tras la verificación UC autorizada, crear credencial/ubicación externa, el catálogo `tt_ecommerce_synthetic`, los esquemas vacíos `bronze`, `silver`, `quarantine` y `gold`, y grants base. El IaC declara para el único grupo escritor `USE_CATALOG`, `USE_SCHEMA` y `CREATE_TABLE`, sin declarar `SELECT`/`MODIFY` ni grants para otros grupos; inventariar accesos heredados/propietarios antes de inferir acceso efectivo. Todo debe existir antes de cargar datos.
9. Mantener el SQL Warehouse como configuración/código opt-in desactivado en Fase 1: no crearlo ni habilitarlo. Su creación, auto-stop y gate de coste se trasladan explícitamente a Fase 5, cuando exista Gold y se confirme precio/uso de Power BI.

**Entregables:** diseño de módulos Terraform, guía de despliegue/rollback, nombre no versionado del único grupo escritor aprobado, decisión y runbook de backend, evidencia autorizada de la fundación/UC base o registro explícito de bloqueos, y lista de soporte/permisos necesarios para UC.

**Aceptación verificable:** además de la validación offline registrada, un despliegue autorizado demuestra la fundación Azure, metastore asignado, identidad aplicadora y el parent external location/child path exclusivo de managed storage de catálogo, sin mutar el root DEFAULT vacío. Antes de recibir datos existen credencial/ubicación externa, `tt_ecommerce_synthetic`, sus cuatro esquemas vacíos y los grants **explícitos de IaC** del único grupo escritor (`USE_CATALOG`, `USE_SCHEMA`, `CREATE_TABLE`). Antes y después del apply se inventarían grants y propietarios preexistentes/efectivos: el IaC no revoca herencias, y el propietario/creador puede tener poderes adicionales, incluidos lectura/modificación de sus tablas. Se comprueba acceso efectivo y propietarios con identidades permitidas/no autorizadas, incluidas negativas de lectura/escritura para BI, stewardship y analistas, sin cargar datos en Fase 1. No se exige ni se declara máscara, PII real ni SQL Warehouse.

---

### Fase 2 — Bronze incremental, recuperación y observabilidad básica

**Objetivo:** cargar origen sintético sin mutarlo y poder reanudar la ingestión sin duplicar resultados lógicos.

**Estado: CIERRE LOCAL TERMINADO (2026-11-09) Y PRIMERA INGESTA CLOUD EJECUTADA UNA ÚNICA VEZ (evidencia reportada por el operador); VALIDACIÓN COMPLETA SIN FORMALIZAR.** El acta de decisiones de Fase 2 está aprobada como **contrato canónico** y la implementación de código está **terminada y commiteada** (287 pruebas `unittest` en 13 suites, en verde; Terraform 1.16.6 y Databricks CLI 1.22.1 instalados; `terraform validate` de `bootstrap`/`platform` OK en el host de preparación, mientras que el de `infra/databricks/` está bloqueado ahí por Application Control al iniciar el provider y corresponde al operador desde su host autorizado). **Nota de divergencia de hito controlada (portfolio):** el cierre del hito se priorizó sobre la formalización de la validación completa; la ingesta cloud de `DEMO-001` se ejecutó una única vez y fue verificada de forma independiente en lectura (recuentos 26/11/7, con anomalías, procedencia, checkpoints y run-log presentes), y **Bronze queda congelado** desde entonces — sin re-ejecuciones, sin mutar `landing` y sin resetear checkpoints. La validación completa no está formalizada: ningún gate GF2 firmado, RE-PA-1/RE-PA-2 sin evidencia en su ventana (§4 del borrador de cierre), P-2/P-3 diferidos y el cierre por excepción ([borrador sin firmar](fase-2/EVIDENCIA_CLOUD_DEMO-001_CIERRE_EXCEPCION_BORRADOR.md)); **no se declara Fase 2 cerrada**. El trabajo restante es la formalización del cierre (firmas/atestiguamientos externos) y, si se requiere, completar P-2/P-3 y las bitácoras pendientes. El hardening del cierre local fija, además: root `abfss://landing@<account>.dfs.core.windows.net` fijado por despliegue (variable del bundle pasada a la tarea como argumento fijo, validado estrictamente como raíz exacta del contenedor por `layout.validate_landing_root` y nunca parámetro editable del run — el job expone solo `batch_id`/`arrival_date` con `timeout_seconds: 3600`); directorio de batch por fuente auditado exacto antes de DDL (listado metadata-only que exige contener solo el archivo aprobado; extra o subdirectorio rechaza el lote antes de leer bytes); Auto Loader filtrado al filename contractualmente aprobado (`pathGlobFilter` con nombre literal-validado, leyendo siempre el directorio del lote); y `CREATE_TABLE` sobre `bronze` reservado al run-as del job (el grupo escritor conserva `CREATE_TABLE` solo en `silver`/`quarantine`/`gold`). Documentación: [arquitectura y decisiones](fase-2/ARQUITECTURA_Y_DECISIONES.md) · [gates y aprobaciones previas](fase-2/GATES_DESPLIEGUE.md) · [runbook de validación](fase-2/RUNBOOK_VALIDACION.md) · [plantilla de evidencia cloud](fase-2/EVIDENCIA_CLOUD_TEMPLATE.md). Decisiones registradas en el acta: exclusivamente datos sintéticos (primer lote previsto `DEMO-001`; manifest declara 44 filas); aterrizaje en dos etapas con rutas canónicas — subida a `landing/_staging/<batch_id>/` verificada contra manifest → landing final inmutable con layout `landing/<source>/arrival_date=YYYY-MM-DD/batch_id=<batch_id>/<source>_<batch_id>.<ext>`, manifest de control en `landing/_control/manifests/<batch_id>/manifest_<batch_id>.json` y archivo ready `landing/_control/ready/<batch_id>.json` escrito al final (commit); tres tablas Bronze raw gestionadas y append-only (`bronze.orders_raw`/`bronze.customers_raw`/`bronze.products_raw`) más un log de ejecución append-only (`bronze.ingestion_run_log`, una fila por intento y fuente); Auto Loader por fuente con trigger `availableNow`, esquemas explícitos por contrato, drift capturado en `_rescued_data`, línea malformada con evidencia en `_corrupt_record`, audit column `batch_id` y procedencia en `_metadata` (ruta física del archivo, tamaño, mtime); `parsed_payload_canonical` es contenido parseado/reconstruido, no byte-exacto (los bytes exactos solo viven en la landing inmutable, acreditados por SHA-256 del manifest); columnas físicas de la fuente en texto, con tipado semántico y calidad reservados a la Fase 3; `cleanSource = off`; estado del pipeline en el volumen UC lógicamente direccionado como `pipeline_state.ingestion_state` con rutas `/Volumes/tt_ecommerce_synthetic/pipeline_state/ingestion_state/<source>/{schema,checkpoints/<batch>}`, físico y aislado de las rutas de entrada del lote en landing y del almacenamiento de tablas, con checkpoints retenidos; deduplicación y recuperación dependen del checkpoint retenido e intacto — si se pierde o se sustituye, **no se reclama recuperación**; no hay exactly-once atómico entre tablas (la semántica de checkpoint del sink cubre las tablas raw; el log puede duplicar registros de intento tras un retry); sin mutar Bronze; el SQL Warehouse permanece desactivado.

**Aprobaciones requeridas antes de la primera ingesta cloud (cada una explícita y registrada; detalle en los gates):** service principal dedicado como run-as del job, que junto con el volumen de estado es el único primer creador/propietario de las tablas raw y del log (verificación preflight); Access Connector nuevo con rol Reader en el alcance del **contenedor** `landing` (no se reclama que aísle la identidad Azure) + credencial UC y **cuatro external locations de subruta** (`orders`, `customers`, `products`, `_control`) recibidas con `READ_FILES` únicamente por la identidad del job; el job queda fuera de la lectura de `_staging` (subida y promoción exclusivas del productor/operador); grants del volumen UC `pipeline_state.ingestion_state` con sus rutas canónicas; Terraform solo declara `CREATE_TABLE` sobre `bronze` para el run-as del job —las tablas raw y `bronze.ingestion_run_log` las crea y posee él en la primera ingesta, sin precreación por Terraform/admin y sin ninguna grant extra `SELECT`/`MODIFY`—, con la invariante de propiedad verificada en preflight; tres planes/aplicaciones Terraform secuenciados y autorizados por separado (layout → conector → fundación UC), cada uno con plan revisado sin destrucciones/reemplazos de Fase 1/Stage 2 y con parada/reconocimiento del estado tras un fallo parcial (no es transaccional; sin `destroy`/`-target`); los flags de habilitación/aprobación de Fase 2 quedan operacionalmente irreversibles tras el apply y su retirada exige un cambio revisado por separado; subida autorizada del lote `DEMO-001` (solo productor/operador); y aprobación explícita de coste/configuración del cómputo efímero para **cada** ejecución de prueba. Según el reporte del operador, el ciclo cloud se ejecutó con estas condiciones vigentes en el runtime; ninguna firma o atestiguamiento externo quedó registrado y ningún gate queda firmado.

**Dependencias:** Fases 0–1; ADLS y workspace desplegados autorizadamente; metastore UC asignado, parent external location y child path de managed storage de catálogo aprobados, credencial/ubicación externa, `tt_ecommerce_synthetic`, sus esquemas/grant base y los permisos de ingesta verificados antes de cargar Bronze.

**Tareas unitarias**

1. **Completado (local):** implementar el aterrizaje en dos etapas con rutas canónicas: `_staging` verificado (SHA-256 y recuentos contra manifest: 26 líneas físicas de orders —25 parseables + 1 malformada—, 11 de customers, 7 de products; 44 declaradas), promoción al layout final `landing/<source>/arrival_date=YYYY-MM-DD/batch_id=<batch_id>/<source>_<batch_id>.<ext>`, commit con manifest de control en `_control/manifests/<batch_id>/` y archivo ready en `_control/ready/<batch_id>.json` escrito al final; landing final inmutable y mínimo privilegio definido entre identidades (operador, pipeline, ninguno más). Materializado en `pipelines/bronze/promotion.py` + `scripts/promote_batch.py` (`plan`/`publish`, con verificación descarga+SHA-256 tras cada upload y ready como última escritura).
2. **Completado (local):** implementar Auto Loader por fuente con trigger `availableNow`, esquema explícito por contrato Fase 0, ubicación de esquema y checkpoint por fuente/batch en `pipeline_state.ingestion_state` (`/Volumes/tt_ecommerce_synthetic/pipeline_state/ingestion_state/<source>/{schema,checkpoints/<batch>}`, físicamente aislado de landing y tablas, checkpoints retenidos), procedencia en `batch_id` y `_metadata`, `parsed_payload_canonical` parseado (no byte-exacto), columnas físicas de la fuente en texto, `_rescued_data` para drift, `_corrupt_record` para malformados y `cleanSource = off`, con el hardening del cierre: stream acotada por `pathGlobFilter` al filename aprobado y directorio del lote auditado exacto antes de DDL.
3. **Completado (local):** escribir las tres tablas Bronze/raw Delta gestionadas append-only (`bronze.orders_raw`, `bronze.customers_raw`, `bronze.products_raw`) con `batch_id`, sistema fuente, `_metadata` de archivo, timestamps y auditoría; añadir el log de ejecución append-only `bronze.ingestion_run_log` (eventos por intento y fuente, admitiendo intentos duplicados tras retry); con el run-as del job como único primer creador/propietario —Terraform concede `CREATE_TABLE` sobre `bronze` solo al run-as (el grupo escritor lo conserva en `silver`/`quarantine`/`gold`), sin precreación por Terraform ni admin y sin `SELECT`/`MODIFY` adicionales—; nunca sobrescribir el payload raw.
4. **Completado (local):** registrar métricas por ejecución en el log: solo métricas observables (`numInputRows` con `metrics_observed`) por fuente; el run-log no infiere filas escritas/rescatadas/corruptas/leídas ni archivos; ventana, identificadores y estado.
5. **Completado (local):** probar reinicio desde checkpoint retenido e intacto y reproceso controlado con el mismo lote; documentar que si el checkpoint se pierde o se sustituye **no se afirma recuperación** (el reproceso pasa a ser una ingesta nueva con duplicados documentados) y que no existe exactly-once atómico entre tablas; dejar definida la estrategia de deduplicación de Fase 3 por lectura lógica, sin borrar evidencia Bronze.
6. **Completado (local):** simular una columna inesperada y una fila malformada (`extra_json_field`/`ORD-EXTRA-01` y `malformed_json`/`ORD-MALF-01` del fixture); verificar alerta/registro y preservación para diagnóstico (`_rescued_data`/`_corrupt_record`).
7. **Ejecutado (cloud, según reporte del operador; formalización pendiente):** la primera ingesta de `DEMO-001` se ejecutó una única vez y fue verificada de forma independiente en lectura (recuentos 26/11/7, run-log y checkpoints presentes). La evidencia resultante se registra en el [borrador de cierre por excepción sin firmar](fase-2/EVIDENCIA_CLOUD_DEMO-001_CIERRE_EXCEPCION_BORRADOR.md): ningún gate queda firmado y las bitácoras de sesión siguen pendientes de relleno/firma externa.

**Entregables:** implementación local **terminada** del núcleo Bronze (`pipelines/bronze/`), CLI operatoria de promoción (`scripts/promote_batch.py`), root script del job (`scripts/run_bronze_job.py`), bundle Databricks (`databricks.yml`, `resources/bronze_job.yml`) y las suites de pruebas (287 pruebas en verde en el estado actual del repositorio, 13 suites), configuración declarada de esquema/checkpoint en el volumen `pipeline_state.ingestion_state`, definición declarada de las tablas `bronze.<fuente>_raw` y del log `bronze.ingestion_run_log`, documentación de fase (acta, gates, runbook y [plantilla de evidencia](fase-2/EVIDENCIA_CLOUD_TEMPLATE.md)), y evidencia de validación registrada como **reporte del operador** en el [borrador de cierre por excepción sin firmar](fase-2/EVIDENCIA_CLOUD_DEMO-001_CIERRE_EXCEPCION_BORRADOR.md) (sin firmar; no constituye validación completa).

**Aceptación verificable:** dos ejecuciones sucesivas sin nuevos archivos no incrementan Bronze y sí añaden filas de intento en `bronze.ingestion_run_log`; tras reinicio con checkpoint retenido e intacto, el job continúa desde su checkpoint sin duplicar filas raw por el reinicio; no se reclama recuperación con checkpoint perdido/sustituido ni exactly-once atómico entre tablas; `batch_id`, `_metadata` (ruta física del archivo, tamaño, mtime) y timestamp de ingestión son consultables; `parsed_payload_canonical` queda identificado como contenido parseado, no byte-exacto, y la acreditación de bytes exactos permanece en la landing inmutable vía SHA-256; el drift no se pierde silenciosamente y queda visible en `_rescued_data`/`_corrupt_record` y en métricas; las evidencias mínimas por escenario están en el [runbook](fase-2/RUNBOOK_VALIDACION.md). Con la ejecución cloud registrada como reporte del operador, los criterios observados en lectura (recuentos, run-log, checkpoints, propiedad del run-as) quedan consignados como **hechos reportados** en el borrador de cierre; su formalización firmada sigue pendiente, y P-2/P-3 (idempotencia/recuperación) continúan **sin ejecutar**.

---

### Fase 3 — Silver, calidad, deduplicación y cuarentena

**Objetivo:** producir datos conformados y joinables con reglas explícitas, sin promover errores a consumidores.

**Estado: HITO `DEMO-001` EJECUTADO EN CLOUD Y VERIFICADO EN LECTURA (evidencia reportada por el operador; detalle en el [acta de Fase 3](fase-3/ACTA_EVIDENCIA_CLOUD_DEMO-001.md)).** **Divergencia controlada de alcance del hito (portfolio):** en lugar de las tareas genéricas 1–7 siguientes, el hito materializó **una instantánea completa del lote `DEMO-001`** mediante `replaceWhere` acotado por scope con timestamp de auditoría determinista — sin streaming/CDC/SCD/MERGE/incremental genérico y sin atomicidad entre tablas de salida — con Silver current/cuarentena/Gold k-anónimo. Resultado reportado: Silver orders/customers/products **15/10/7** filas, cuarentena **6 rechazos esperados**, Gold con **una celda privacy-safe vigente** (pala / ES-MD / web = **640,00 EUR** / **5 pedidos** / **0 líneas reembolsadas**; controles pre-supresión 812,20 EUR / 13 / 2), estado `COMPLETED_WITH_QUARANTINE`, **todos los chequeos del verificador read-only en verde** y **Gold sin PII ni identificadores**; primer run `673784339635889` fallido antes de tocar Bronze (falta de lectura de workspace-files) y sin crear outputs; job desplegado solo manual (sin schedule), ID `87818861015609`; SP dedicado de Silver `3d25b147-73a3-4c2f-878b-1d6f0d886c2e` con grants Terraform commiteadas en `983e7c0` y aplicadas (9 altas; 0 cambios/destrucciones). **Trabajo diferido:** las tareas genéricas 1–7, el diseño incremental/CDC/SCD, la orquestación programada y el consumo SQL/BI (Fase 5 sin iniciar).

**Dependencias:** Fase 2 y contratos aprobados de Fase 0.

**Tareas unitarias** (las tareas originales permanecen como alcance completo de la fase; el hito `DEMO-001` las difiere):

1. Implementar lectura incremental de Bronze y normalización de tipos, fechas, moneda, estados y códigos de categoría/región.
2. Definir clave de deduplicación de pedidos (`order_id` + `line_id`) y orden determinista por versión/evento e ingestión; aplicar `MERGE` idempotente y conservar trazabilidad hacia Bronze. Las claves de clientes y productos se fijarán en sus contratos.
3. Añadir `created_at`, `updated_at`, `deleted_at` (soft delete cuando aplique), `source_system`, identificador de lote y resultado de calidad a cada fila Silver.
4. Aplicar reglas de Fase 0 antes de publicar: separar válidas, inválidas y casos pendientes de revisión en cuarentena con código y detalle de fallo; mantener Silver y cuarentena restringidos desde su creación según la matriz de Fase 1.
5. Implementar controles de contrato y tests: esquema, nulabilidad, unicidad, referencias a clientes/productos, rango y frescura.
6. Generar métricas de calidad por lote y bloquear o marcar el refresco Gold cuando fallen reglas críticas.
7. Ejecutar casos de prueba para duplicado, corrección, soft delete, nulo permitido, nulo rechazado, referencia inexistente y llegada tardía.

**Entregables:** pipelines Silver/cuarentena, suite de pruebas, reporte de calidad y runbook de recuperación.

**Aceptación verificable:** reejecutar el mismo lote deja el mismo estado Silver; cada fila de cuarentena tiene origen y motivo; una regla crítica fallida impide la promoción a Gold; todas las tablas Silver contienen las auditorías definidas; joins de referencia aprobados pasan sus tests. **Nota de estado:** con el hito ejecutado, esta aceptación queda acreditada de forma acotada al lote `DEMO-001` según el reporte del operador (re-ejecución reemplaza exactamente el scope, cuarentena con origen y motivo, Gold sin PII, verificador read-only en verde); la cobertura genérica completa de la fase sigue abierta y es trabajo diferido.

---

### Fase 4 — Unity Catalog, privilegios y verificación por identidad

**Objetivo:** aplicar gobernanza efectiva de datos, no solo un diseño de permisos.

**Dependencias:** Fase 1 y tablas de Fases 2–3.

**Tareas unitarias**

1. Validar y completar propiedad, catálogos y esquemas de Fase 1 para Bronze, Silver, cuarentena y Gold; cualquier objeto Gold adicional se prepara al existir sus tablas.
2. Completar los privilegios mínimos sobre los controles base de Fase 1: ingeniería para operación, stewardship con caso justificado para contacto original, `silver-analysts-authorized` solo sobre una vista/subconjunto Silver aprobado y `bi-executives` solo sobre Gold publicado.
3. Mantener Silver restringido; no conceder `SELECT` general a analistas. Publicar una vista específica para el subconjunto permitido y reservar las tablas base a los grupos operativos/autorizados.
4. Configurar y probar la política de enmascaramiento dinámico para email/teléfono sintéticos con pertenencia a grupos reales: ingenieros/stewards autorizados ven el original; analistas autorizados reciben valor enmascarado. No se carga ni trata PII real en esta fase; mantener una ruta segura de mantenimiento de grupo.
5. Verificar con identidades reales de prueba separadas: positivo para ingeniero/steward autorizado (original), positivo para analista autorizado (vista y máscara), positivo para BI/dirección (Gold), y negativo para cada acceso no permitido a Bronze/Silver/cuarentena. Verificar además que la política de máscara no otorga `SELECT` a una identidad sin privilegio.
6. Revisar logs/auditoría disponibles y documentar cómo investigar un acceso o un cambio de privilegios.

**Entregables:** matriz de acceso implementada, políticas UC, evidencias de pruebas por identidad y procedimiento de revisión de permisos.

**Aceptación verificable:** `bi-executives` no puede consultar Bronze, Silver ni cuarentena y sí puede leer Gold publicado; una identidad de ingeniería/steward con autorización recibe email/teléfono originales; una identidad de `silver-analysts-authorized` solo puede leer la vista/subconjunto permitido y recibe contacto enmascarado; identidades no autorizadas fallan las consultas; el administrador no se usa como evidencia y ninguna máscara actúa como sustituto de `GRANT`/restricción de tablas.

---

### Fase 5 — Gold con criterios de publicación y consumo BI

**Objetivo:** entregar métricas estables, útiles y con exposición controlada.

**Dependencias:** Fases 3–4, validación de métricas por negocio y gate de coste/permisos del SQL Warehouse aprobado tras existir Gold.

**Tareas unitarias**

1. Construir agregados Gold de ingresos/pedidos por fecha, categoría, región y estado, sin columnas PII previstas.
2. Refrescar una ventana de fechas explícita contra una instantánea completa de Silver; usar sobrescritura selectiva con restricción de predicado activa para que una fecha sin ventas elimine resultados antiguos sin tocar otras fechas.
3. Asociar a cada fila Gold un resultado/puntuación de calidad y timestamp de refresco; definir el tratamiento para datos incompletos.
4. Aplicar los criterios acordados de umbral, supresión o generalización y registrar las celdas no publicadas. Revisar que no se promete ausencia absoluta de riesgo de inferencia.
5. Crear vistas Gold de consumo y permisos para BI. Solo entonces revisar precio, cuota, permisos y patrón de uso del SQL Warehouse; crear el Warehouse opt-in con capacidad conservadora y auto-stop, y validar consultas.
6. Construir un informe Power BI inicial con modo Import después de validar el Warehouse. Evaluar DirectQuery solo si los requisitos de frescura lo justifican y tras medir comportamiento/coste.
7. Comparar resultados con un conjunto de control conocido: ventas completadas, reembolsos y días sin ventas.

**Entregables:** tablas/vistas Gold, definición de métricas, evidencia de refresco de ventana, consulta de validación e informe Power BI MVP.

**Aceptación verificable:** un reembolso o cancelación elimina/recalcula correctamente la métrica en la ventana afectada; una fecha vacía no conserva una fila de ingresos antigua; fechas fuera de la ventana no cambian; antes de Power BI se evidencia el gate de coste y el Warehouse creado tiene auto-stop; BI solo consulta los objetos Gold autorizados; el informe coincide con el conjunto de control.

---

### Fase 6 — Pruebas, operación, documentación y coste

**Objetivo:** hacer el MVP repetible, mantenible y evaluable por un reclutador o revisor técnico.

**Dependencias:** Fases 0–5.

**Tareas unitarias**

1. Añadir pruebas unitarias de transformaciones, integración de lote sintético y pruebas negativas de contrato/calidad/permisos.
2. Configurar CI para formato, validación Terraform, tests sin credenciales reales y revisión de secretos; no ejecutar despliegues destructivos por defecto.
3. Documentar parámetros, secuencia de despliegue, rollback, recuperación de checkpoint, reproceso, cuarentena y contacto responsable.
4. Definir métricas operativas: frescura, duración, filas, fallos de calidad, porcentaje rescatado y coste/tiempo de cómputo; establecer umbrales iniciales y canal de alerta a validar.
5. Revisar gasto real de almacenamiento, jobs y SQL Warehouse; ajustar autoapagado, tamaños y frecuencia sin fijar cifras no medidas.
6. Revisar minimización, retención, borrado y auditoría como requisitos pendientes, y registrar decisiones/no decisiones en el README o ADRs futuros.

**Entregables:** pipeline CI, suite de evidencia, runbooks, guía de coste y documentación final del portfolio.

**Aceptación verificable:** una ejecución limpia del repositorio valida código sin secretos; un revisor puede seguir el runbook y reproducir el flujo con datos sintéticos; hay evidencia de pruebas de calidad y permisos; el documento identifica explícitamente recursos no desplegados y controles aún no implementados.

## Stretch goals (posteriores al MVP)

| Iniciativa | Dependencia | Criterio de finalización |
| --- | --- | --- |
| CDC/streaming de pedidos | Bronze y modelo de eventos estables | Semántica de idempotencia, watermark y eventos tardíos probada con replay. |
| SCD tipo 2 | Claves y cambios de dimensiones definidos | Historial de clientes/productos consultable con fechas de vigencia y tests. |
| Orquestación y alertas | Jobs MVP instrumentados | SLA, dependencia entre tareas, reintentos y alerta probados ante fallo simulado. |
| Lineage/catálogo ampliado | UC y contratos consolidados | Propietarios, SLA y descripción visibles para todos los objetos Gold. |
| Endurecimiento de privacidad | Criterios de publicación validados | Revisión de combinaciones de dimensiones, umbrales y proceso de excepción documentados. |
| Optimización | Patrones de consulta y volumen medidos | Particionado/clustering y modo Power BI escogidos con evidencia de rendimiento y coste. |

## Hitos de decisión abiertos

1. Confirmar tenant, France Central, presupuesto/capacidad administrativa y evidencia de las decisiones G5 y coste todavía pendientes.
2. Ejecutar y evidenciar, solo tras autorización, el backend remoto decidido: bootstrap, RBAC Blob de contenedor, propagación y migración de estado; no se reabre la decisión sin un motivo de seguridad/coste.
3. Validar la configuración concreta de Unity Catalog y qué partes puede automatizar la versión de provider disponible.
4. Definir volumen objetivo y dimensionamiento de infraestructura; el SLA objetivo, ingresos, ventana de reproceso y umbral Gold ya se acordaron en Fase 0, pero no se han medido ni implementado.
5. Decidir si Power BI necesita DirectQuery; el punto de partida es Import por simplicidad y control de coste.
6. Definir políticas de retención/borrado y el responsable de aprobar cualquier uso futuro de datos no sintéticos.
7. Aprobar y registrar, antes de la primera ingesta cloud de la Fase 2, los gates GF2 de su acta: identidad run-as dedicada (único primer creador/propietario de Bronze; Terraform solo con `CREATE_TABLE`, sin precreación), conector Reader en el contenedor `landing` y las cuatro external locations de subruta con `READ_FILES` limitado al job (sin acceso a `_staging`), volumen UC `pipeline_state.ingestion_state`, planes/aplicaciones Terraform secuenciados y autorizados por separado con plan revisado 0-destrucciones de Fase 1/Stage 2, flags de Fase 2 irreversibles tras el apply, subida autorizada de `DEMO-001` y cómputo efímero aprobado por cada ejecución de prueba. *(Estado: la primera ingesta se ejecutó una única vez según el reporte del operador y los gates siguen sin firmar; su formalización forma parte del cierre por excepción sin firmar de Fase 2.)*
