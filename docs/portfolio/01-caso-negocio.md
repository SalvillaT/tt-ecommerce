# 01 — Caso de negocio: la métrica y su control de privacidad

**Pregunta de negocio:** ¿cuánto ingresa hoy, por producto/región/canal, sin exponer datos de clientes a quien solo consume analítica?

## La métrica publicada

La celda Gold vigente para la instantánea `DEMO-001`, en el grano `(business_date, categoría, región, canal)`:

| Campo | Valor publicado |
| --- | --- |
| Categoría | pala |
| Región | ES-MD |
| Canal | web |
| Ingresos netos | **640,00 EUR** |
| Pedidos | **5** |
| Líneas reembolsadas | **0** |

- Es la **única celda física** publicada por el hito: no existe fila para la otra celda candidata (goma/rubber), que queda **suprimida** por el criterio de privacidad.
- Los **controles previos a supresión** registrados por el job para esa dimensión: **812,20 EUR / 13 / 2** (importe, pedidos y líneas reembolsadas antes de aplicar el umbral). La diferencia frente a la celda publicada es el efecto explícito del gating, no una discrepancia silenciosa.
- **Gold no contiene PII ni identificadores** — verificado por el verificador read-only del propio job ([`scripts/verify_silver_demo_001.py`](../../scripts/verify_silver_demo_001.py), chequeo `gold_published_paddle_cell_no_rubber` y ausencia de columnas PII/identificador en `DESCRIBE TABLE`).

## Por qué es privacy-safe

- El criterio de publicación acordado en Fase 0 solo publica celdas canónicas con **k ≥ 5 sujetos distintos** y sin roll-ups solapados; las demás se suprimen. Regla `PUB-001` en [`docs/fase-0/METRICAS_Y_CALIDAD.md`](../fase-0/METRICAS_Y_CALIDAD.md) y decisiones de linaje en [`docs/fase-0/LINAJE_Y_PRIVACIDAD.md`](../fase-0/LINAJE_Y_PRIVACIDAD.md).
- El propio generador sintético construye el caso de prueba: un segmento baseline publicable (k = 5) y otro suprimible, con los escenarios negativos en celdas propias ([`scripts/generate_synthetic_data.py`](../../scripts/generate_synthetic_data.py), sección "Privacidad Gold (PUB-001) por construcción").
- **Los campos con forma de PII del fixture son datos de prueba 100 % sintéticos** —correo, teléfono e identificadores de cliente generados determinísticamente (dominios de prueba `*.example.test`, teléfonos ficticios `+3400…`): no corresponden a ninguna persona real, y el control de privacidad se demuestra sin tratar datos reales en ningún momento.
- Se declara sin rodeos: el criterio **no elimina** el riesgo residual de reidentificación (inferencia sobre celdas de baja cardinalidad); es un control de publicación, no una certificación.

## Recorrido de la métrica (trazabilidad completa)

```text
Contrato Fase 0 (docs/fase-0) → generador determinista + manifest SHA-256
  → Bronze append-only (payload crudo, anomalías preservadas; congelado tras su única ingesta)
  → Silver current + cuarentena (rechazos explicables, nunca corrección silenciosa)
  → Gold k-anónimo (solo celdas que superan el umbral)
  → verificación read-only independiente, todo en verde
```

- Contratos y reglas: [`docs/fase-0/CONTRATOS.md`](../fase-0/CONTRATOS.md).
- Núcleo determinista de la transformación (stdlib, sin PySpark): [`pipelines/silver/core.py`](../../pipelines/silver/core.py).
- La cadena Bronze→Gold **no muta Bronze**: Silver lee las tablas raw en solo lectura; Bronze `DEMO-001` se ingirió una vez y queda congelado.

## Condición de la evidencia

Los valores anteriores (640/5/0 y 812.20/13/2, Silver 15/10/7, cuarentena 6) son **resultados del run cloud ejecutado, observados por el operador** (salidas de consola del verificador y del run report): evidencia `operator-reported`, **no reproducible desde Git** ni audit-formal. El acta con el detalle completo: [`docs/fase-3/ACTA_EVIDENCIA_CLOUD_DEMO-001.md`](../fase-3/ACTA_EVIDENCIA_CLOUD_DEMO-001.md). La lógica que los produce está commiteada y está cubierta por las pruebas locales de [`tests/test_silver_core.py`](../../tests/test_silver_core.py) y [`tests/test_silver_verifier.py`](../../tests/test_silver_verifier.py).

Siguiente: [02-correccion-del-pipeline.md](02-correccion-del-pipeline.md) — qué falló en cloud y cómo se corrigió.
