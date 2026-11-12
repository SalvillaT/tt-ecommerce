# Dossier de portfolio — Lakehouse gobernado en Databricks (TT E-commerce)

Lectura rápida para reclutamiento: en 30 segundos se ve el resultado de negocio; en 90 segundos, el recorrido completo de prueba. El proyecto demuestra, con datos sintéticos y un lote único `DEMO-001`, cómo pasar de un **contrato de datos** a una **métrica publicada privacy-safe** sobre Azure Databricks + Unity Catalog + Terraform.

## En 30 segundos

**Resultado de negocio (celda Gold publicada, vigente):** categoría **pala** / región **ES-MD** / canal **web** = **640,00 EUR** de ingresos netos, **5 pedidos**, **0 líneas reembolsadas** — la única celda que supera el umbral de privacidad (k ≥ 5); los controles previos a supresión registran **812,20 EUR / 13 / 2**. Todo el flujo contrato → Bronze → Silver/cuarentena → Gold se ejecutó una vez, se verificó de forma independiente en solo lectura y Gold **no contiene PII ni identificadores**.

## En 90 segundos

| Paso | Prueba | Dónde |
| --- | --- | --- |
| 1. Caso de negocio y métrica | Celda publicada + gating de privacidad | [01-caso-negocio.md](01-caso-negocio.md) |
| 2. Corrección del pipeline (2 incidencias reales resueltas) | Wrapper Bronze y bootstrap Silver | [02-correccion-del-pipeline.md](02-correccion-del-pipeline.md) |
| 3. Jobs de Databricks y ejecución | Bundle manual serverless, 2 tareas, verificador en verde | [03-databricks-jobs-y-ejecucion.md](03-databricks-jobs-y-ejecucion.md) |
| 4. Unity Catalog y gobierno | IaC de mínimo privilegio, SP dedicado separado | [04-unity-catalog-y-gobierno.md](04-unity-catalog-y-gobierno.md) |
| 5. Límites y siguientes pasos | Honestidad de alcance declarada | [05-limites-y-siguientes-pasos.md](05-limites-y-siguientes-pasos.md) |
| 6. Radiografía del proyecto | Mapa repo → claim → tests | [06-radiografia-del-proyecto.md](06-radiografia-del-proyecto.md) |
| Evidencia visual (capturas pendientes) | Checklist de captura/redacción | [evidence/README.md](evidence/README.md) |

Verificación local reproducible por cualquier lector (287 pruebas `unittest` en 13 suites, sin credenciales cloud):

```powershell
py -m unittest discover -s tests -v
```

## Honestidad de alcance (léela antes de citar cifras)

- **Evidencia commiteada** (reproducible desde Git): código, bundle, Terraform, fixture y las 287 pruebas locales. Esto es lo único que este repositorio acredita por sí mismo.
- **Datos 100 % sintéticos, con forma de PII pero sin ninguna PII real:** todos los campos del fixture con forma de dato personal —correo, teléfono, identificadores de cliente— son **datos de prueba 100 % sintéticos** generados por el generador determinista (dominios de prueba `*.example.test`, teléfonos ficticios `+3400…`). No corresponden a ninguna persona real, no se cargan datos reales en la plataforma y todo el control de privacidad se demuestra sin tratar PII real en ningún momento.
- **Evidencia cloud = reportada por el operador**: los resultados de ejecución en Databricks (ingesta Bronze única y hito Silver) proceden de observación humana y salidas de consola suministradas en la sesión de documentación; **no son reproducibles desde Git** ni constituyen evidencia de auditoría formal. Se etiquetan como `operator-reported` en todo el dossier.
- **Fase 2 NO está cerrada formalmente**: su cierre por excepción sigue **sin firmar**; RE-PA-1/RE-PA-2 sin evidencia en su ventana y P-2/P-3 diferidos. Bronze `DEMO-001` se ingirió **una vez** y quedó **congelado** (sin re-run, sin mutar `landing`, sin resetear checkpoints).
- **La evidencia Silver es un hito acotado**: job manual, instantánea fija del lote vía `replaceWhere` por scope y timestamp de auditoría determinista. **No** es streaming, CDC, SCD, incremental genérico ni `MERGE`, y **no** es atómico entre tablas de salida.
- **No iniciado**: SQL Warehouse, Power BI, dashboard en vivo, CI/CD, monitorización de producción, máscaras dinámicas ni prueba de identidad de consumidor.
- Las **capturas de pantalla** de este dossier aún **no existen**: están pendientes de captura manual autenticada por el operador, con redacción de identificadores (véase [evidence/README.md](evidence/README.md)). No hay imágenes provisionales.

## Mapa del dossier

1. [01-caso-negocio.md](01-caso-negocio.md) — la métrica de negocio y por qué es privacy-safe.
2. [02-correccion-del-pipeline.md](02-correccion-del-pipeline.md) — dos incidencias reales y su corrección versionada.
3. [03-databricks-jobs-y-ejecucion.md](03-databricks-jobs-y-ejecucion.md) — bundle, job de 2 tareas, ejecución y controles exactos.
4. [04-unity-catalog-y-gobierno.md](04-unity-catalog-y-gobierno.md) — mínimo privilegio con IaC y separación de identidades.
5. [05-limites-y-siguientes-pasos.md](05-limites-y-siguientes-pasos.md) — lo que NO se reclama y el roadmap.
6. [06-radiografia-del-proyecto.md](06-radiografia-del-proyecto.md) — mapa técnico completo con rutas versionadas.
7. [evidence/README.md](evidence/README.md) + [evidence/evidence-index.yml](evidence/evidence-index.yml) — plan de captura de las 6 capturas pendientes.

Demo visual estático del resultado (contenido fijo, sin llamadas cloud, sin SQL Warehouse ni Power BI): [`demo/index.html`](../../demo/index.html) — instrucciones en [`demo/README.md`](../../demo/README.md).

Referencias históricas detalladas por fase: [codemap raíz](../../codemap.md), [README técnico](../../README.md), [acta de evidencia cloud del hito Silver](../fase-3/ACTA_EVIDENCIA_CLOUD_DEMO-001.md) y [borrador de cierre por excepción de Fase 2 (sin firmar)](../fase-2/EVIDENCIA_CLOUD_DEMO-001_CIERRE_EXCEPCION_BORRADOR.md).
