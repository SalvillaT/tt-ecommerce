# Fase 2 — Borrador de cierre por excepción de la validación cloud (lote DEMO-001)

**Estado: BORRADOR SIN FIRMAR.** Este documento es una propuesta de redacción de cierre **por excepción calificada** de la validación cloud de Fase 2 para el lote `DEMO-001`. **No constituye evidencia, no registra aprobación alguna, no se firmó y NO declara que Fase 2 esté cerrada.** El cierre real, en su caso, exige el relleno de todos los campos pendientes por el operador desde evidencia en su poder, la revisión del repositorio y la firma **externa** de la autoridad humana competente. Hasta entonces, la fase permanece `SIN FIRMAR` y cualquier afirmación de cierre es nula.

**Naturaleza de este borrador:**

- Documentación pura, acotada a este archivo. No se ejecutó ninguna llamada cloud (Azure, Databricks ni otra) para producirlo; todo lo aquí consignado procede del **reporte del operador** y de su **evidencia redactada, en poder del operador y externa a este repositorio**. Este repositorio por sí solo no constituye evidencia cloud directa.
- Sigue la higiene de la [plantilla de evidencia](EVIDENCIA_CLOUD_TEMPLATE.md): **ningún identificador real** entra en Git. Todo valor sensible se expresa con placeholders: `<subscription>`, `<tenant>`, `<storage_account>`, `<workspace_url>`, `<sp-application-id>`, `<job_id>`, `<run_id>`, `<task_run_id>`, `<pipeline_id>`, `<attempt_id>`, `<endpoint_id>`, etc. Sin secretos, tokens, cadenas de conexión ni contenido de datos crudo: los datos del lote se citan solo por sus recuentos ya públicos del contrato (26/11/7, 44) y sus identificadores de escenario sintético canónicos (`ORD-EXTRA-01`, `ORD-MALF-01`, `SYN-CAMP-01`), que son parte del fixture documentado, no datos reales.
- No sustituye a la [plantilla de evidencia](EVIDENCIA_CLOUD_TEMPLATE.md) ni a sus bitácoras de sesión: las bitácoras firmadas siguen siendo el conducto de evidencia. Este borrador organiza el **cierre de conjunto** y las excepciones.

---

## 1. Índice de evidencia (campos a rellenar por el operador)

Cada fila la completa el operador **por sesión autorizada**, referenciando artefactos **redactados y alojados fuera de Git** (en el almacenamiento de evidencia del operador). Un campo sin rellenar deja la fila `PENDIENTE` y el bloque correspondiente en `SIN FIRMAR`.

| # | Elemento | Tipo | Ubicación (fuera de Git) | SHA-256 del artefacto redactado | Timestamp (UTC) | Responsable | Estado |
| --- | --- | --- | --- | --- | --- | --- | --- |
| E-01 | Bitácora de sesión de despliegue (GF2-0…GF2-5) | Bitácora | `<ruta_evidencia_operador>` | `<sha256_redactado>` | `<YYYY-MM-DDThh:mm:ssZ>` | `<operador>` | `<PENDIENTE/DISPONIBLE>` |
| E-02 | Resúmenes de los tres planes Terraform redactados (layout → conector → fundación UC) con hashes | Plan JSON redactado | `<ruta_evidencia_operador>` | `<sha256_redactado>` | `<YYYY-MM-DDThh:mm:ssZ>` | `<operador>` | `<PENDIENTE/DISPONIBLE>` |
| E-03 | Evidencia de identidad y acceso (GF2-1/GF2-2): positivos y negativos | Capturas / salidas | `<ruta_evidencia_operador>` | `<sha256_redactado>` | `<YYYY-MM-DDThh:mm:ssZ>` | `<operador>` | `<PENDIENTE/DISPONIBLE>` |
| E-04 | Plan JSON de promoción y eventos de publicación en orden (P-0) | JSON | `<ruta_evidencia_operador>` | `<sha256_redactado>` | `<YYYY-MM-DDThh:mm:ssZ>` | `<productor/operador>` | `<PENDIENTE/DISPONIBLE>` |
| E-05 | Registro de la primera ingesta (P-1): recuentos pre/post, ejecución del job, salida del wrapper con el fallo aparente conocido | Salidas / capturas | `<ruta_evidencia_operador>` | `<sha256_redactado>` | `<YYYY-MM-DDThh:mm:ssZ>` | `<operador>` | `<PENDIENTE/DISPONIBLE>` |
| E-06 | Consulta del run-log `bronze.ingestion_run_log` con la secuencia de eventos de la primera ingesta | Salida de consulta | `<ruta_evidencia_operador>` | `<sha256_redactado>` | `<YYYY-MM-DDThh:mm:ssZ>` | `<operador>` | `<PENDIENTE/DISPONIBLE>` |
| E-07 | Resultado de la tarea de verificación read-only posterior (SUCCESS): P-1 redactado, P-4 y P-5 por predicados sin payload | Salida / captura | `<ruta_evidencia_operador>` | `<sha256_redactado>` | `<YYYY-MM-DDThh:mm:ssZ>` | `<operador>` | `<PENDIENTE/DISPONIBLE>` |
| E-08 | Listados metadata-only de checkpoints y esquemas por fuente en `pipeline_state.ingestion_state` | Salida de listado | `<ruta_evidencia_operador>` | `<sha256_redactado>` | `<YYYY-MM-DDThh:mm:ssZ>` | `<operador>` | `<PENDIENTE/DISPONIBLE>` |
| E-09 | Propietario efectivo de las tablas gestionadas e inventario de grants heredados (GF2-4) | Salida de consulta UC | `<ruta_evidencia_operador>` | `<sha256_redactado>` | `<YYYY-MM-DDThh:mm:ssZ>` | `<operador>` | `<PENDIENTE/DISPONIBLE>` |
| E-10 | Cambio del wrapper (retorno natural) desplegado: PR/revisión redactada sin IDs reales | Referencia de cambio | `<ruta_evidencia_operador>` | `<sha256_redactado>` | `<YYYY-MM-DDThh:mm:ssZ>` | `<operador/mantenedor>` | `<PENDIENTE/DISPONIBLE>` |
| E-11 | Acta/waiver de las excepciones RE-PA-1/RE-PA-2 (§4) | Acta firmada externamente | `<ruta_evidencia_operador>` | `<sha256_redactado>` | `<YYYY-MM-DDThh:mm:ssZ>` | `<autoridad>` | `<PENDIENTE/FIRMADA>` |
| E-12 | Coste observado de la sesión y constatación de cero compute activo / SQL Warehouse no arrancado | Captura Azure Cost Analysis / estado | `<ruta_evidencia_operador>` | `<sha256_redactado>` | `<YYYY-MM-DDThh:mm:ssZ>` | `<operador>` | `<PENDIENTE/DISPONIBLE>` |

---

## 2. Resultado factual conocido (forma redactada, a rellenar por el operador)

Los hechos de esta sección se consignan **tal como fueron reportados por el operador**, en forma redactada y sin identificadores reales. La distinción entre «hecho verificado» y «pendiente de adjuntar/firmar» se marca en cada punto. **Ningún ítem de esta sección se da por firmado en este borrador.»

### 2.1 Primera ingesta (P-1): trabajo de pipeline completado, fallo aparente solo del wrapper

| # | Hecho factual | Verificado | Pendiente |
| --- | --- | --- | --- |
| F-01 | La ejecución de la primera ingesta (`bundle run` con `--batch_id DEMO-001` y `<arrival-date>`, job `<job_id>`, run `<run_id>`, intento `<attempt_id>` en el workspace `<workspace_url>` de la suscripción `<subscription>`/tenant `<tenant>`) **completó el trabajo del pipeline**: el proceso de ingesta ejecutó su ciclo y generó el registro de eventos y las filas Bronze previstas por el contrato. | Reportado por el operador; hallazgos confirmados por la verificación read-only (§2.3) | Adjuntar salidas del run y del wrapper (E-05) y consulta del run-log (E-06) |
| F-02 | La **tarea del job** terminó con un estado aparente de fallo causado **exclusivamente por el wrapper**: `SystemExit(0)` emitido por el contenedor del script del workspace después de que el trabajo real del pipeline hubiera terminado, en lugar de retornar de forma natural. Está catalogado como **falso fallo del wrapper, no del pipeline**; el pipeline terminó el trabajo de ingesta previsto. | Reportado por el operador; diagnóstico coherente con la salida del wrapper (E-05) | Adjuntar salida exacta del wrapper y la clasificación documentada del incidente |
| F-03 | En consecuencia, el estado de terminación de esa tarea corresponde a una **excepción conocida del wrapper del job**, no a un fallo de ingesta ni a un evento `FAILED` del run-log. La distinción queda consignada aquí y en la bitácora de sesión. | Hecho documentado en este borrador | Valorarse en el acta de cierre (§7) como parte del cierre por excepción |

### 2.2 Wrapper de retorno natural

| # | Hecho factual | Verificado | Pendiente |
| --- | --- | --- | --- |
| F-04 | Se materializó y desplegó un **cambio del wrapper de ingesta** para que la tarea del workspace **retorne de forma natural** al final del trabajo del pipeline, eliminando la emisión que provoca el falso fallo `SystemExit(0)` de F-02. El cambio es posterior a la primera ingesta y su evidencia es un artefacto del ciclo de desarrollo del job (E-10). | Cambio en el repositorio y reportado como desplegado para el job de ingesta | Acreditar el deploy y, si se exige, una futura ejecución de ingesta autorizada que use el cambio; la verificación read-only no ejecuta este wrapper |

### 2.3 Verificación read-only posterior: SUCCESS

Una tarea de verificación **manual, de solo lectura** —separada de la primera ingesta y ejecutada después— terminó con estado **SUCCESS**. Consultó Bronze sin ejecutar ingesta ni mutar nada; los listados de estado son una evidencia metadata-only separada. La tarea de verificación no ejecuta el wrapper de ingesta de F-04. Lo que acreditó, en su salida redactada y en poder del operador:

| # | Hecho factual | Verificado | Pendiente |
| --- | --- | --- | --- |
| F-05 | **Terminación SUCCESS** de la tarea de verificación read-only (`<task_run_id>` en el workspace `<workspace_url>`), que retorna de forma natural. Es una tarea separada y no acredita por sí misma el wrapper de ingesta de F-04. | Ejecución observada por el operador | Adjuntar salida redactada (E-07) |
| F-06 | **Recuentos exactos 26/11/7** sobre Bronze: `orders` con 26 líneas físicas (25 parseables + 1 malformada), `customers` 11, `products` 7; 44 declaradas por el manifest. Coincide con el contrato del lote `DEMO-001`. | Consulta Bronze read-only | Adjuntar consulta y salida (E-07) |
| F-07 | En `orders` se observaron **una fila rescatada** y **una fila corrupta**, con la distribución de estados esperada; ambos escenarios no se descartaron silenciosamente. Predicados redactados posteriores devolvieron exactamente un match para el escenario de rescate y uno para el marcador corrupto canónicos, sin emitir payload. | Tarea de verificación read-only SUCCESS; salida pendiente de adjuntar como E-07 | Enlazar la salida redactada con los bloques P-4/P-5; la adjunción y firma externas siguen pendientes (§6) |
| F-08 | **Estados esperados** de la ejecución de ingesta reflejados en el run-log; en particular el evento `SOURCE_COMMITTED` de cada fuente queda en el **estado acordado** para las anomalías (rescate y malformación no son fallo de fuente). | Consulta del run-log | Adjuntar filas del log redactadas |
| F-09 | **Secuencia esperada de cinco eventos y sin `FAILED`** para la primera ingesta: `STARTED`, `MANIFEST_VALIDATED` (por run, sin fuente) y `SOURCE_COMMITTED` ×3 (orders, customers, products); **cero eventos `FAILED`**. Sin duplicados inesperados de eventos para ese run. | Consulta del run-log | Adjuntar filas del log redactadas |
| F-10 | **Identificadores de pipeline coincidentes**: los identificadores de pipeline (`<pipeline_id>`, `<run_id>`, `<attempt_id>`) registrados por la verificación read-only coinciden con los de la ejecución de ingesta de F-01; esos identificadores quedan redactados en la evidencia del operador. Eso acredita que la verificación examinó **la misma ejecución y sus mismos artefactos de estado**, no un artefacto ajeno. | Consulta corroborada en read-only | Adjuntar lado a lado de identificadores redactados (E-05, E-07) |
| F-11 | **Tablas Bronze gestionadas y propiedad del run-as**: las tablas raw y el run-log son tablas **UC gestionadas**, y su **propietario efectivo es únicamente el service principal del job** (`<sp-application-id>`); sin precreación por Terraform ni por un administrador. Corresponde al resultado esperado de GF2-4. | Consulta UC read-only | Adjuntar fila de propiedad e inventario de grants heredados (E-09) |
| F-12 | **Checkpoints y esquemas por fuente retenidos**: el listado en `pipeline_state.ingestion_state` muestra, por cada fuente (`orders`, `customers`, `products`), los artefactos `schema` y `checkpoints/DEMO-001` **retenidos** en la ubicación canónica `/Volumes/tt_ecommerce_synthetic/pipeline_state/ingestion_state/<source>/…`. | Listado metadata-only read-only | Adjuntar listados por fuente (E-08) y evidencia separada de cero artefactos de estado en `landing` o en el almacenamiento de tablas, si se pretende firmar GF2-3 |

### 2.4 Lectura conjunta de §2

- Los ítems F-05…F-12 constituyen hallazgos **factualmente observados en una verificación read-only SUCCESS**, según el reporte del operador; su evidencia descansa en artefactos y consultas **en poder del operador, redactados, fuera de Git**.
- La sección §2 **no firma** ningún gate ni bloque de la [plantilla](EVIDENCIA_CLOUD_TEMPLATE.md): sitúa los hechos y señala qué artefacto falta por adjuntar/atestiguar en cada caso. Los bloques de las bitácoras de sesión quedan `SIN FIRMAR` hasta que el operador los rellene desde la evidencia y la autoridad humana los apruebe.

---

## 3. Artefactos GF2 / P-0 / P-1 a adjuntar o atestiguar

Lista estructurada de los artefactos mínimos que las bitácoras de sesión correspondientes deben adjuntar en la evidencia del operador o que el operador debe **atestiguar** (declarar bajo su responsabilidad que existen, con su ubicación fuera de Git). Nada de ello está aquí marcado como aportado.

### 3.1 Gates GF2 (despliegue)

- **GF2-0 — Alcance:** confirmación registrada de que la sesión usó exclusivamente el lote sintético `DEMO-001` con manifest, bajo las condiciones G5 vigentes (datos sintéticos, sin PII ni datos reales; endpoints públicos sin allowlist, principals autenticados).
- **GF2-1 — Identidad:** registro del SP dedicado del job (`<sp-application-id>`, tenant `<tenant>`) como run-as, sin roles admin ni acceso general a suscripción, separado de operador, principal Terraform y conectores existentes.
- **GF2-2 — Acceso a landing:** evidencia de las verificaciones **positivas** (lectura del job en las cuatro external locations de subruta sobre el contenedor `landing` de `<storage_account>`) y **negativas** (job sin acceso a `_staging`; sin acceso fuera de `landing`; identidad no autorizada sin acceso), más el inventario de locations/grants solapados revisado antes de cada plan.
- **GF2-3 — Estado:** volumen `pipeline_state.ingestion_state` con rutas canónicas por fuente y batch; cero artefactos de estado en `landing`; checkpoints retenidos (refuerzo factual: F-12).
- **GF2-4 — Posesión:** consulta UC de inexistencia de las tablas antes de la primera ejecución; propiedad efectiva post-ingesta = run-as (refuerzo factual: F-11), con inventario de grants heredados.
- **GF2-5 — IaC:** los tres planes/aplicaciones secuenciados (layout → conector Reader + Azure RBAC/credencial → fundación UC), cada uno con su **hash de plan redactado**, 0 destrucciones y 0 reemplazos de objetos de Fase 1/Stage 2, y una **aprobación separada por plan**; sin `destroy` ni `-target`.
- **GF2-6 — Promoción:** plan JSON (P-0) y eventos de publicación en orden con verificación SHA-256 tras cada upload; manifest de control en `landing/_control/manifests/DEMO-001/` y ready `landing/_control/ready/DEMO-001.json` como última escritura; `_staging` intacto.
- **GF2-7 — Cómputo:** aprobación de coste/configuración **por cada ejecución** de `bundle run` (tipo, tamaño, autoterminación; `timeout_seconds: 3600`), coste observado al cierre y constatación de cero compute activo / SQL Warehouse no arrancado.
- **GF2-8 — Revisión humana:** registro de la revisión conjunta previa a la primera ingesta (coste, RBAC/grants, rutas, nombres, manifest de auditoría, rollback, destinatarios de alertas) y su veredicto.
- **GF2-9 — Irreversibilidad de flags:** constatación de que los flags de Fase 2 quedaron declarados operacionalmente irreversibles tras el apply; ningún plan los resetea.
- **GF2-10 — Job/bundle/concurrencia:** `bundle validate` estático y `deploy` con `run_as_service_principal_name=<sp-application-id>` y `landing_root=abfss://landing@<storage_account>.dfs.core.windows.net` fijado **antes del deploy**; job sin schedule, `max_concurrent_runs=1`, `max_retries=0`, `timeout_seconds=3600`; parámetros de run solo `batch_id`/`arrival_date`.

### 3.2 Pruebas usadas por este cierre

- **P-0 — Preparación y promoción del lote:** plan JSON (`promotion_plan_DEMO-001.json` redactado), eventos JSON por paso en orden, recuentos/SHA-256 de verificación, listado de archivos promovidos; condición `SIN FIRMAR` hasta relleno.
- **P-1 — Primera ingesta:** recuentos pre/post, comando `bundle run` con marcadores redactados, salidas del wrapper (incluidas F-01…F-03), filas del run-log (F-08…F-10), procedencia (`batch_id`, `_metadata`) enlazada al punto 6, coste de esta ejecución; condición `SIN FIRMAR` hasta relleno y **condicionada al tratamiento de las excepciones de §4**.
- **GF2 vinculado:** los puntos de §2 refuerzan, pero **no sustituyen**, los bloques GF2-3 (F-12), GF2-4 (F-11) y la evidencia del run-log de P-1 (F-08…F-10).

---

## 4. Excepciones históricas RE-PA-1 y RE-PA-2

**Naturaleza de la excepción.** RE-PA-1 y RE-PA-2 son **requisitos de evidencia con condición temporal obligatoria**, definidos en la [plantilla](EVIDENCIA_CLOUD_TEMPLATE.md) y el [runbook](RUNBOOK_VALIDACION.md):

- **RE-PA-1** (auditoría negativa en runtime del subdirectorio con nombre exacto aprobado) debía ejecutarse en el runtime autorizado **después de los applies y antes de la primera ingesta**, contra un directorio de control creado y retirado con autorización expresa.
- **RE-PA-2** (único escritor entre preflight y `availableNow`) debía evidenciarse **durante la ventana de la primera ingesta**: listado metadata-only inicial en el preflight, cero escrituras ajenas durante el deploy/run y constatación final al concluido el run, con la nota expresa de que **no se promete atomicidad SHA→lectura**.

**Excepciones registradas en este borrador:**

- E-01 — **RE-PA-1 no queda acreditado con evidencia firmada dentro de su ventana temporal para la primera ingesta registrada en este borrador.** La condición «post-apply y pre-primera-ingesta» es de **ventana irrepetible**: la primera ingesta ya se ejecutó, y una re-ejecución de la auditoría ahora **no replicaría ni la ventana ni el estado pre-ingesta** (las tablas existen y el checkpoint está retenido). En consecuencia, **este borrador no afirma pase retroactivo de RE-PA-1** y no puede marcar P-1 como validado por esa vía.
- E-02 — **RE-PA-2 no queda acreditado con evidencia firmada de su ventana para la misma ingesta.** Igual que en RE-PA-1, la ventana preflight → `availableNow` de esa ejecución ya transcurrió; los listados y constataciones posteriores en read-only **no equivalen** al registro en su momento. **Este borrador no afirma pase retroactivo de RE-PA-2**: la garantía operativa de único escritor, para esa ejecución, **no está evidenciada según las reglas**.

**Tratamiento del cierre por excepción:**

1. Ambas excepciones quedan **registradas como huecos de evidencia históricos** en este borrador y, tras la revisión, en el cierre final si procede.
2. El pase real o la renuncia a exigirlos **exige un acto separado de control de cambios (waiver/acta)**, firmado por la autoridad humana externa, que declare expresamente: (a) qué se renuncia a evidenciar y por qué; (b) los riesgos asumidos de la ventana no evidenciada (garantía de único escritor no evidenciada para esa ejecución, sin declaración de atomicidad SHA→lectura); (c) cualquier **ingesta futura** queda **sujeta a RE-PA-1/RE-PA-2 firmados en su ventana**, sin heredar la exención.
3. **Ningún checkbox de la plantilla ni de este borrador marca pase retroactivo.** La firma del waiver es puerta del cierre (§7), no un resultado automático.

---

## 5. P-2 y P-3: diferidos, requieren autorización separada

- **P-2 (reproceso sin archivos nuevos / idempotencia)** y **P-3 (recuperación desde checkpoint)** **no se ejecutaron dentro del alcance cubierto por este borrador y quedan diferidos.** Este documento no registra su ejecución y **no afirma su cobertura**.
- Ambas pruebas implican **nuevas ejecuciones del job** (cómputo) en el runtime autorizado y, por tanto, exigen: gates GF2 vigentes, **aprobación de cómputo por ejecución** (GF2-7), una **autorización separada** para ejecutarlas (que este borrador no otorga ni solicita) y sus bitácoras de sesión propias.
- La retención e integridad de los checkpoints por fuente (F-12) es una **condición previa favorable** registrada como hecho de verificación read-only; **no sustituye** la ejecución firmada de P-2/P-3 ni la acredita por anticipado.

---

## 6. P-4, P-5 y procedencia de P-1: reporte read-only disponible, pendiente de adjunción/firma

- Los hallazgos F-07 (rescatada/corrupta) tocan los objetivos de **P-4 (drift de esquema)** y **P-5 (registro malformado)**, y la procedencia de las filas (`batch_id`, `_metadata` con ruta física/tamaño/mtime y timestamp coherente) pertenece a la evidencia de **P-1**.
- La tarea read-only posterior produjo el reporte de esas comprobaciones: procedencia P-1 mediante agregados redactados y P-4/P-5 mediante predicados sin payload. El operador debe alojar su salida redactada fuera de Git y enlazarla como E-07; este borrador no contiene ni sustituye ese artefacto.
- En consecuencia, este borrador **no declara P-1, P-4 ni P-5 firmados**: los marca como **cubiertos factualmente** por la salida read-only y **pendientes de adjuntar/atestiguar** en sus bloques de la plantilla. Hasta entonces, los bloques permanecen `SIN FIRMAR`.

---

## 7. Redacción propuesta de la decisión de cierre (solo cierre por excepción calificada)

La redacción siguiente es una **plantilla de articulado para el acta**, con todos los campos de firma, autoridad, fecha y coste como placeholders. **Nada de ello está aprobado ni firmado en este borrador.** Si algún campo queda sin rellenar, el cierre no procede y la fase queda `SIN FIRMAR`.

> **Propuesta de decisión — cierre de Fase 2 cloud por excepción calificada (BORRADOR, sin firmar)**
>
> 1. La revisión humana designada («la Revisión») ha examinado el reporte del operador y la evidencia redactada referenciada en el índice (§1), correspondiente a la validación cloud del lote `DEMO-001` (suscripción `<subscription>`, tenant `<tenant>`, workspace `<workspace_url>`).
> 2. La Revisión constata, como hechos documentados: (a) el trabajo de la primera ingesta se completó y su terminación aparente en fallo corresponde a una **excepción conocida y acotada del wrapper del job** (F-01…F-03); (b) un **cambio del wrapper de ingesta a retorno natural** fue desplegado, sin que la verificación read-only separada se presente como prueba de ese wrapper (F-04); (c) la verificación read-only posterior terminó **SUCCESS** y acreditó los recuentos exactos **26/11/7** con una fila rescatada y una corrupta en orders, los estados esperados, la secuencia esperada de cinco eventos del run-log sin `FAILED`, identificadores de pipeline coincidentes, tablas gestionadas con propiedad efectiva del run-as y checkpoints/esquemas por fuente retenidos (F-05…F-12).
> 3. La Revisión **no certifica la validación completa de Fase 2**: RE-PA-1 y RE-PA-2 **no quedaron evidenciados en su ventana** para esa ingesta (§4) y **P-2 y P-3 quedan diferidos** (§5). El reporte read-only de P-1/P-4/P-5 está disponible, pero requiere adjunción y atestiguamiento externos (§6). El alcance de esta decisión es, exclusivamente, un **cierre por excepción calificada** del estado fáctico anterior.
> 4. En virtud de lo anterior, la autoridad competente decide cerrar **por excepción calificada** el registro de la validación cloud del lote `DEMO-001`, bajo las condiciones siguientes: (a) el waiver/acta de control de cambios sobre las excepciones RE-PA-1/RE-PA-2 de §4 queda firmado y adjuntado como E-11; (b) cualquier ingesta futura queda sujeta a RE-PA-1/RE-PA-2 firmados en su ventana y a aprobación de cómputo por ejecución; (c) P-2/P-3 siguen diferidos hasta su autorización separada; (d) el reporte read-only separado se adjunta como E-07 para dar soporte a los hallazgos de P-1/P-4/P-5.
> 5. Autoridad y firma (todos los campos pendientes del acta externa): aprobador `<nombre_y_rol>` con autoridad `<campo_autoridad>`; fecha/hora UTC `<YYYY-MM-DDThh:mm:ssZ>`; responsable ejecutor `<operador>`; revisora `<identificador_de_acta>`. **Sin la firma externa de la autoridad humana este acto no existe.**
> 6. Coste y estado del runtime al cierre: coste observado de la(s) sesión(es) `<importe>` (aprobaciones por ejecución de GF2-7 con su límite `timeout_seconds=3600`); constatación de **cero compute activo** `<captura/referencia>`; **SQL Warehouse no arrancado** `<captura/referencia>`; sin schedules activos.
> 7. Efecto de la decisión: consigna el cierre **por excepción calificada** del lote `DEMO-001` con las excepciones y diferidos de §4–§6 listados; **no declara Fase 2 cerrada por validación completa** y no habilita por sí mismo ninguna ejecución futura sin sus gates y aprobaciones.

**Advertencia reiterada:** si en el acta final se omite cualquiera de los elementos 5 o 6, o si se redacta «cierre» sin calificar «por excepción calificada», el resultado **no es el cierre definido en este borrador** y debe tratarse como nulo/borrador revisable.

---

## 8. Lo que este borrador NO afirma

- **No declara Fase 2 cerrada.** Declara un borrador **hacia una decisión de cierre por excepción** cuya firma es externa y no ha ocurrido.
- **No afirma pase retroactivo** de RE-PA-1 ni de RE-PA-2; su renuncia o pase exige waiver/acta y no se presume.
- **No firmó ningún gate GF2, bloque RE-PA o prueba P-x** de la plantilla; los deja `SIN FIRMAR` hasta que el operador los rellene desde la evidencia y se firmen externamente.
- **No afirma cobertura completa de P-2 ni P-3**; los registra como diferidos. Los hallazgos read-only de P-1/P-4/P-5 requieren todavía adjunción y firma externas antes de cualquier declaración de cobertura firmada.
- **No inventa aprobación, autoridad, firma, fecha, coste ni estado de runtime**: todos quedan con placeholders o marcados `PENDIENTE`/`SIN FIRMAR`.
- **No contiene identificadores reales, secretos ni datos crudos**: solo placeholders y los recuentos e identificadores de escenario contractuales ya documentados.
