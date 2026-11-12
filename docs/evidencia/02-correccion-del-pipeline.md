# 02 — Corrección del pipeline: dos incidencias reales en cloud y su arreglo versionado

Los pipelines de workspace fallan por razones de plataforma, no solo de lógica. Durante el ciclo cloud registrado (según reporte del operador) se produjeron dos incidencias de esta clase; ambas se diagnosticaron y corrigieron con cambios versionados. Este documento es el relato técnico de esas correcciones.

## Incidencia 1 — Falso fallo del wrapper Bronze (`SystemExit(0)`)

**Síntoma.** La primera ingesta Bronze de `DEMO-001` **completó su trabajo** (filas y run-log generados), pero la tarea terminó con estado de fallo: el contenedor del script del workspace emitía `SystemExit(0)` al final, en lugar de retornar de forma natural, y la plataforma lo interpretó como terminación anómala.

**Diagnóstico.** Fallo exclusivo del wrapper, no del pipeline: el proceso de ingesta había terminado su ciclo antes de la emisión. Documentado como excepción conocida en el [borrador de cierre por excepción de Fase 2 (sin firmar)](../fase-2/EVIDENCIA_CLOUD_DEMO-001_CIERRE_EXCEPCION_BORRADOR.md), §2.1–§2.2.

**Corrección versionada.** El root script del job Bronze ahora **retorna de forma natural** al terminar el trabajo del pipeline: [`scripts/run_bronze_job.py`](../../scripts/run_bronze_job.py) (commit `3dfb5d5` del repositorio). La verificación read-only de la fase es una tarea separada y no ejercita ese wrapper; su despliegue efectivo para ingestas futuras queda sujeto a sus propias autorizaciones.

**Lección:** separar el estado del trabajo real del estado del contenedor que lo ejecuta, y documentar la excepción en vez de reintentar a ciegas.

## Incidencia 2 — Run Silver fallido antes de tocar Bronze (lectura de workspace-files)

**Síntoma.** El primer run del job de materialización Silver **falló antes de acceder a Bronze**, por falta de lectura de los archivos de workspace del bundle. **No creó ningún output**: sin tablas, sin filas, sin reporte parcial — el diseño fail-fast evitó estado huérfano.

**Diagnóstico.** Al ejecutarse como script de workspace (`source: WORKSPACE`), Python no trae la raíz del proyecto en `sys.path` y los imports de `pipelines` no resuelven.

**Corrección versionada.** El root script Silver bootstrappea la ruta con un **argumento interno fijo de la tarea** —no parámetro editable del run— inyectado por el bundle y validado antes de importar `pipelines`:

- [`scripts/run_silver_demo_001_job.py`](../../scripts/run_silver_demo_001_job.py) — inserta `scripts/` y la raíz del proyecto en `sys.path`, luego despoja el argumento interno de `sys.argv`.
- [`resources/silver_demo_001_job.yml`](../../resources/silver_demo_001_job.yml) — el argumento `--pipelines-root=${workspace.file_path}` va fijado en la configuración de la tarea, comentado como "NOT a run-editable parameter".

**Resultado tras la corrección:** el run siguiente completó la materialización y el verificador read-only terminó con **todos los chequeos en verde** (según reporte del operador). Detalle en el [acta de evidencia del hito Silver](../fase-3/ACTA_EVIDENCIA_CLOUD_DEMO-001.md), §4–§5.

**Lección:** el bootstrap de ejecución es parte del contrato del job; se fija como configuración de despliegue y se valida explícitamente, igual que el resto de parámetros no editables.

## Invariantes que sobrevivieron a ambas incidencias

- **Bronze congelado:** la ingesta Bronze se ejecutó una única vez; después, ni re-runs, ni mutaciones de `landing`, ni reseteo de checkpoints (requisito registrado en el cierre de Fase 2).
- **Fail-fast sin outputs parciales:** el run fallido de Silver no dejó tablas a medio escribir.
- **Verificación independiente:** cada hito termina con un verificador read-only separado, no con la aserción del propio job.

## Condición de la evidencia

Ambas incidencias y sus correcciones en runtime son **evidencia reportada por el operador** (salidas de consola y observación humana): no reproducibles desde Git ni audit-formales. Los cambios de código sí son conmutados y están en el historial del repositorio.

Siguiente: [03-databricks-jobs-y-ejecucion.md](03-databricks-jobs-y-ejecucion.md) — el job en sí y sus controles.
