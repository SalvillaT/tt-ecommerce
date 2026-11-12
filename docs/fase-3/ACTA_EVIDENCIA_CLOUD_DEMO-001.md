# Fase 3 — Acta de evidencia cloud del hito Silver `DEMO-001`

**Naturaleza de la evidencia.** Todo lo consignado aquí como resultado cloud es **evidencia reportada por el operador**: salidas de consola suministradas en la sesión de documentación, **no re-ejecutadas ni re-verificadas desde este repositorio**. Se distingue explícitamente de la **evidencia commiteada del repositorio** (código, bundle, Terraform y las **287 pruebas `unittest` locales en verde**, sin credenciales cloud), que no acredita ejecución en el workspace. Este documento **no firma gates, no cierra Fase 2** —su [cierre por excepción](../fase-2/EVIDENCIA_CLOUD_DEMO-001_CIERRE_EXCEPCION_BORRADOR.md) permanece **sin firmar**— y no inventa aprobación, autoridad ni firma alguna. **Nota de redacción (revisión de seguridad):** los identificadores operativos reales de Databricks —application ID del SP dedicado de Silver, job ID y IDs de run— se consignan con marcadores semánticos (`<silver-service-principal>`, `<silver-materialization-job>`, `<failed-pre-access-run>`, `<successful-silver-run>`); los valores reales se omiten del árbol actual y permanecen en la evidencia externa al repositorio. Esta redacción no altera commits históricos previos.

---

## 1. Resumen ejecutivo

El hito `DEMO-001` de Fase 3 materializó **una instantánea completa y fija del lote** en el lakehouse gobernado (Silver current + cuarentena + Gold k-anónimo) sobre Databricks con Unity Catalog, con verificación read-only independiente y sin tocar Bronze:

| Control | Resultado reportado |
| --- | --- |
| Silver orders / customers / products | **15 / 10 / 7** filas |
| Cuarentena | **6 rechazos esperados** (por contrato del fixture) |
| Gold — celda vigente privacy-safe | pala / ES-MD / web = **640,00 EUR** / **5 pedidos** / **0 líneas reembolsadas** |
| Controles pre-supresión | **812,20 EUR / 13 / 2** |
| Estado del run report | `COMPLETED_WITH_QUARANTINE` |
| Verificador read-only | **todos los chequeos en verde** |
| Gold | **sin PII ni identificadores** |

Alcance declarado sin ambigüedad: **no** es streaming, CDC, SCD, incremental genérico ni `MERGE`; **no** es atómico entre tablas de salida. **Sin iniciar:** SQL Warehouse, Power BI, dashboard, CI/runbooks finales y cualquier diseño incremental genérico.

## 2. Precondiciones

| # | Precondición | Detalle registrado |
| --- | --- | --- |
| P-01 | **Bronze congelado y verificado.** El lote `DEMO-001` se ingirió una única vez en Fase 2 y fue verificado de forma independiente en lectura (recuentos raw 26/11/7; evidencia de anomalías, procedencia, checkpoints y run-log). Ver [cierre por excepción de Fase 2](../fase-2/EVIDENCIA_CLOUD_DEMO-001_CIERRE_EXCEPCION_BORRADOR.md), §9. | Evidencia operador (Bronze) |
| P-02 | **Identidad dedicada de Silver.** Service principal Entra/Databricks **dedicado**, citado en esta acta con el marcador semántico `<silver-service-principal>` (por revisión de seguridad, el valor real del application ID no se publica en Git; no es un secreto, pero queda en evidencia externa al repositorio). | Evidencia operador |
| P-03 | **Grants de mínimo privilegio Fase 3 aplicados vía Terraform**: commiteados en `983e7c0` y aplicados con **9 altas; 0 cambios; 0 destrucciones**. | Commiteado en el repositorio + apply reportado |
| P-04 | **Separación humana/SP:** el propietario UC humano conserva **`Service principal: User`** sobre el SP (supervisión operativa, sin implicar rol en plano de datos); el SP tiene **solo `Can View`** sobre el subárbol `files` del bundle. | Evidencia operador |

## 3. Despliegue del job del bundle

- Despliegue **manual únicamente, sin schedule**; job ID `<silver-materialization-job>` (marcador semántico; el valor real no se publica en Git).
- **Topología de dos tareas:** (1) tarea de materialización — crea/escribe las tablas gestionadas de salida con `replaceWhere` por scope y escribe `silver.materialization_run_report` al final como marcador de completitud; (2) tarea de **verificación read-only dependiente** — solo `SELECT`/`DESCRIBE` fijos contra las salidas.
- El run-as es el SP dedicado de Silver (P-02); las tablas gestionadas las crea el SP del job, nunca Terraform.

## 4. Run fallido previo al acceso a Bronze

| Hecho | Detalle |
| --- | --- |
| Run `<failed-pre-access-run>` | Falló **antes de tocar Bronze** por falta de lectura de workspace-files del bundle. |
| Impacto | **No creó outputs** ni mutó estado: sin tablas, sin filas y sin reporte parcial. |

Lección operativa registrada: el fallo se produjo en la fase de bootstrap de archivos de workspace, no en la lógica del pipeline ni contra datos.

## 5. Run exitoso (`<successful-silver-run>`) — controles de resultado exactos

Run exitoso sobre la instantánea fija `DEMO-001`, con verificación read-only posterior en verde:

| Control | Resultado reportado |
| --- | --- |
| Silver `orders` / `customers` / `products` | **15 / 10 / 7** filas |
| Cuarentena | **6 rechazos esperados** — rechazos del contrato, no fallos de pipeline |
| Gold — celda canónica vigente publicada | pala / ES-MD / web = **640,00 EUR** / **5 pedidos** / **0 líneas reembolsadas** |
| Controles pre-supresión capturados por el job | **812,20 EUR / 13 / 2** (la celda publicada refleja el umbral/supresión aplicados sobre esos controles) |
| Estado del run report | **`COMPLETED_WITH_QUARANTINE`** (etiqueta explícita de cuarentena; no se declara éxito limpio) |
| Verificador read-only | **todos los chequeos en verde** |
| Gold | **sin PII ni identificadores** |

## 6. Alcance y límites declarados

- **Instantánea completa fija** de `DEMO-001` materializada mediante `replaceWhere` **acotado por scope** (re-ejecutar reemplaza exactamente ese scope, sin duplicados; idempotente por scope) y **timestamp de auditoría determinista**.
- **No** es streaming, CDC, SCD tipo 2, incremental genérico ni `MERGE`: el diseño genérico queda **diferido** (tareas 1–7 del plan de Fase 3).
- **No** es atómico entre tablas de salida: una re-ejecución interrumpida puede dejar tablas en scopes distintos; el reporte final es el marcador de completitud.
- **Despliegue y ejecución manuales únicamente** (sin schedule, sin CDC programado).

## 7. Privacidad

- Gold publica únicamente la celda canónica que supera el umbral de publicación (k ≥ 5); los controles pre-supresión (812,20 EUR / 13 / 2) muestran qué se retuvo antes de publicar 640,00 EUR / 5 / 0.
- **Gold no contiene PII ni identificadores** (verificado por el verificador read-only, chequeos en verde).
- Silver queda restringido por diseño; el SP de Silver tiene el mínimo privilegio de P-03/P-04. No se carga ni trata PII real.

## 8. Invariante de no mutación de Bronze

- La materialización Silver es **read-only contra Bronze**: sin re-run de la ingesta, sin mutación de `landing` y sin reseteo de checkpoints. **Bronze permanece congelado** desde su ingesta única de Fase 2; cualquier ingesta futura exige sus propias autorizaciones y no hereda exenciones.

## 9. Límites de esta evidencia

- Los resultados cloud de §4–§5 son **reporte del operador** (salidas de consola suministradas en la sesión de documentación): este repositorio no los re-ejecutó.
- La evidencia commiteada del repositorio cubre el código, el bundle, los grants Terraform (`983e7c0`) y **287 pruebas locales**; **no** acredita la ejecución cloud.
- Este acta **no firma** gate alguno, **no declara Fase 2 cerrada** (su cierre por excepción sigue sin firmar) y **no habilita** ejecuciones futuras sin sus propias aprobaciones.
