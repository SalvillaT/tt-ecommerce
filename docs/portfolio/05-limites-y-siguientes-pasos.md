# 05 — Límites y siguientes pasos

Un portfolio creíble declara tanto lo que **no** tiene como lo que tiene. Esta es la lista completa de límites, con su estado exacto.

## Límites de evidencia (los más importantes)

| Límite | Estado exacto |
| --- | --- |
| Naturaleza de la evidencia cloud | **Operator-reported**: observación humana y salidas de consola del run documentado; no reproducible desde Git ni evidencia de auditoría formal. |
| Cierre de Fase 2 | **Sin firmar.** El [borrador de cierre por excepción](../fase-2/EVIDENCIA_CLOUD_DEMO-001_CIERRE_EXCEPCION_BORRADOR.md) permanece `SIN FIRMAR`; RE-PA-1/RE-PA-2 (auditoría negativa pre-ingesta y único escritor en ventana) no quedaron evidenciados en su ventana temporal; P-2 (idempotencia/reproceso) y P-3 (recuperación desde checkpoint) quedan **diferidos y sin ejecutar**. |
| Bronze | Ingesta **única** de `DEMO-001`, verificada de forma independiente en lectura (raw 26/11/7); después **congelado**: sin re-run, sin mutar `landing`, sin resetear checkpoints. |
| Silver | Hito `DEMO-001` ejecutado: **no** es streaming, CDC, SCD, incremental genérico ni `MERGE`; **no** es atómico entre tablas de salida; job **manual** sin schedule; instantánea fija vía `replaceWhere` por scope. |
| Capturas de pantalla | **No existen todavía**: pendientes de captura manual autenticada y redacción; el plan está en [evidence/README.md](evidence/README.md). No hay placeholders falsos. |

## Capacidades no iniciadas

- **SQL Warehouse**: código opt-in desactivado; creación diferida a Fase 5 tras gate de coste. Ninguno arrancado.
- **Power BI / dashboard en vivo**: nada construido; existe solo un demo **estático** de contenido fijo ([`demo/index.html`](../../demo/index.html)), sin llamadas cloud ni consultas en directo.
- **CI/CD**: no configurado aún (el plan de Fase 6 lo contempla; nada ejecutándose).
- **Monitorización de producción, alertas, SLA**: no implementados.
- **Máscaras dinámicas y pruebas por identidad** (Fase 4): no configuradas ni ejecutadas; no hay prueba de identidad de consumidor.
- **Diseño incremental/CDC/SCD genérico**: diferido por decisión (el hito acotó el alcance a la instantánea fija del lote).

## Por qué estos límites son razonables

- **Coste controlado**: todo el compute es efímero y manual; el SQL Warehouse permanece apagado hasta que exista Gold y un gate de coste.
- **Alcance del hito elegido**: demostrar el recorrido completo contrato → Gold con verificación independiente en un lote fijo, antes de generalizar. La generalización (incremental/MERGE/streaming/SCD) exige decisiones de frescura y coste que pertenecen a fases posteriores.
- **Gobernanza primero**: los permisos mínimos y la separación de identidades se declararon **antes** de que existieran datos; el flujo de consumo (BI) se abrirá solo después de las pruebas de Fase 4.

## Siguientes pasos (roadmap real)

1. **Formalizar el cierre de Fase 2**: rellenar bitácoras desde la evidencia del operador y firma externa de la autoridad humana (waiver de RE-PA-1/RE-PA-2 y ejecución de P-2/P-3 si se exige). Cualquier ingesta futura requiere sus propios gates.
2. **Completar la evidencia visual** de este dossier con las 6 capturas autenticadas y redactadas ([evidence/README.md](evidence/README.md)).
3. **Fase 4**: matriz de acceso, vistas aprobadas, máscaras dinámicas y verificación por identidad real (positivos y negativos).
4. **Fase 5**: gate de coste → SQL Warehouse opt-in → informe Power BI inicial (Import), con conjunto de control.
5. **Fase 6**: CI (formato, `terraform validate`, tests sin credenciales, revisión de secretos), runbook final y métricas operativas.
6. **Generalizar Silver/Gold**: diseño incremental genérico (con su idempotencia y ventana) solo tras decidir frescura y coste.

Volver al [índice del dossier](README.md).
