# Fase 0 — Métricas y calidad v1

**Navegación:** [contratos](CONTRATOS.md) · [linaje y privacidad](LINAJE_Y_PRIVACIDAD.md). Las reglas son especificación para PySpark/tests futuros, no controles ya ejecutados.

## Métricas Gold acordadas para el MVP

La entrada es la instantánea vigente de Silver: para cada (`order_id`, `line_id`), la fila de mayor `record_version` válida. Si hay empate de versión con distinto payload, no hay fila Silver vigente hasta resolver la cuarentena.

| Métrica | Definición inequívoca |
| --- | --- |
| `total_revenue_eur` | Suma de `net_revenue_eur` de líneas vigentes con estado `COMPLETED` o `PARTIALLY_REFUNDED`, agrupada por `business_date`, categoría de producto, región y canal. `REFUNDED`, `CANCELLED` y `PENDING` aportan 0. |
| `order_count` | `count(distinct order_id)` entre pedidos con al menos una línea vigente `COMPLETED` o `PARTIALLY_REFUNDED` y `net_revenue_eur > 0` en el mismo grupo. Nunca se cuenta una línea como pedido adicional. |
| `refunded_line_count` | Número de líneas vigentes `REFUNDED` o `PARTIALLY_REFUNDED`; indicador separado, no se suma a ingresos. |

`order_count` es distinto **dentro de cada segmento**: el mismo pedido puede aparecer en dos categorías. No se suman conteos entre categorías para calcular el total global; se recalcula el distinto sobre el conjunto global.

El segmento Gold publicable tiene grano (`business_date`, `category`, `region_code`, `channel`). Antes de publicarlo se calcula `privacy_subject_count` sobre `customer_id` distintos de líneas vigentes válidas `COMPLETED`, `PARTIALLY_REFUNDED` o `REFUNDED` que contribuyen a alguna de sus tres medidas. Es un control interno, **no** una columna de salida. Para `k=5` inicial, una celda con menos sujetos no publica ninguna de sus medidas; no se publican totales/subtotales solapados en el MVP. Detalles y riesgo residual: [linaje y privacidad](LINAJE_Y_PRIVACIDAD.md#baseline-de-publicación-gold).

**Ejemplo comprobable (fixture `DEMO-001`, semilla 42):** antes de aplicar la política, el conjunto de líneas vigentes válidas suma `812.20 EUR`, `13` pedidos elegibles distintos y `2` líneas reembolsadas. La celda `(2026-01-10, paddle, ES-MD, web)` reúne cinco clientes distintos y sus medidas publicables serían `640.00 EUR`, `5` pedidos y `0` líneas reembolsadas. La celda `(2026-01-11, rubber, ES-MD, web)` reúne tres clientes distintos: se suprimen **todas** sus medidas, incluidos sus `100.00 EUR`; las demás celdas pequeñas también se suprimen. Estos números son expectativas verificadas por tests locales, **no** resultados de un pipeline Gold desplegado.

La categoría se obtiene de la versión vigente válida de producto y región/canal de la línea de pedido. Si la referencia no existe, la línea queda en cuarentena y no llega a Gold. Si una línea `COMPLETED` pasa a `REFUNDED`, su versión más reciente reemplaza los  ingresos de la versión previa; no se emite un ingreso negativo adicional. Para una devolución parcial solo se agrega el `net_revenue_eur` restante. Una anulación total de líneas pendientes aporta cero y, si deja un pedido sin líneas con neto positivo, lo excluye de `order_count`.

La ventana de refresco acordada está en el [acta F0](CONTRATOS.md#acta-de-decisiones-f0-v1): recalcular una ventana explícita de 7 días y las fechas de negocio afectadas desde una instantánea completa de Silver. Ingresos, estados y `k=5` también están acordados para el MVP. Son reglas de diseño probadas solo con fixture local: no existe aún un Gold, SLA medido ni proceso de refresco desplegado.

## Reglas de calidad

| ID | Severidad | Condición | Destino / efecto | Test esperado |
| --- | --- | --- | --- | --- |
| ORD-001 | crítica | Clave `order_id`/`line_id`, `record_version`, importes, estado, fecha o referencias requeridas nulos/vacíos. | Cuarentena; bloquea Gold para el lote afectado. | Nulo rechazado no entra Silver. |
| ORD-002 | crítica | Clave+versión repetida con payload diferente; versión no entera o `<1`. | Cuarentena de conflicto; alerta. | Dos versiones 2 distintas no se desempatan. |
| ORD-003 | crítica | Enum/transición de estado o aritmética de importes incumplida; moneda distinta de EUR. | Cuarentena. | `COMPLETED` con refund > 0 falla. |
| ORD-004 | crítica | `quantity <= 0`, importe negativo o fuera de rango provisional `0..1,000,000 EUR`. | Cuarentena. | Cantidad 0 falla. |
| ORD-005 | crítica | `customer_id` o `product_id` no existe en maestro vigente válido. | Cuarentena hasta resolver referencia. | Referencia huérfana no llega Gold. |
| ORD-006 | aviso | Mismo payload exacto de clave+versión ya procesado. | Se ignora lógicamente; métrica de duplicados. | Reejecución no altera Silver/Gold. |
| SCH-001 | aviso | Campo JSON desconocido y JSON válido. | Bronze + `rescued data`; no se promueve ni bloquea por sí solo. | Columna extra queda observable. |
| SCH-002 | crítica | JSON malformado, CSV con estructura no parseable o codificación no UTF-8. | Cuarentena de archivo/fila con razón y evidencia raw disponible cuando sea posible; bloquea la parte afectada. | Línea malformada no se interpreta como válida. |
| CUS-001 | crítica | `customer_id`/versión/email/region requeridos inválidos; email no sintético o mal formado. | Cuarentena de cliente; pedidos que lo referencian no se promueven. | Email vacío falla. |
| CUS-002 | aviso | `phone` nulo. | Aceptado, registrado como completitud; no imputar. | Nulo permitido llega Silver. |
| PRD-001 | crítica | Producto sin clave, categoría no permitida, precio < 0, moneda no EUR o `active` inválido. | Cuarentena de producto. | Categoría desconocida falla. |
| REC-001 | crítica | Falla una regla crítica de calidad o la instantánea Silver necesaria no está disponible. | No marcar la actualización Gold como exitosa; revisión operativa. Una fecha sin ventas válidas se trata como vacía y elimina agregados obsoletos, no como error. | Fallo crítico bloquea promoción; fecha vacía borra filas antiguas de esa fecha. |
| PUB-001 | bloqueante por celda | `privacy_subject_count < k` para el grano Gold canónico. | Suprimir **toda** la celda, incluido `refunded_line_count`; registrar el motivo sin publicar identificadores ni contador de sujetos. No es un error de calidad del lote. | Un segmento con 5 clientes distintos se publica; otro con 3 se suprime íntegramente. |

El volumen de infraestructura, límites de nulidad y umbrales de alerta/paging no tienen baseline numérico aún. El SLA acordado es objetivo futuro, no una medición. Indicadores mínimos por lote: filas leídas/válidas/cuarentena/rescatadas/duplicadas, porcentaje por regla, número de conflictos de versión, máximo retraso `ingested_at − event_at` (cuando exista), y fechas Gold recalculadas.

## Escenarios deterministas exigidos al generador

El manifiesto enumera escenarios activados y los tests los localizan por IDs/valores deterministas, no por reloj o aleatoriedad implícita.

| Escenario | Construcción / resultado esperado |
| --- | --- |
| `baseline_valid` | Clientes, productos y líneas válidas; un segmento canónico con 5 `customer_id` distintos y otro con 3 para probar `PUB-001`. El catálogo incluye palas, gomas, pelotas, mesas, redes y accesorios. |
| `exact_duplicate` | Dos eventos byte-equivalentes de una misma versión; ORD-006, una única fila vigente. |
| `later_correction` | Versión 2 válida de la misma línea; sustituye versión 1. |
| `cancellation` | Línea `PENDING` que llega a `CANCELLED`; neto 0, no pedido/ingreso. |
| `partial_refund` / `full_refund` | Revisión de línea completada a neto parcial / 0; sin doble conteo. |
| `late_event` | Evento válido cuya `business_date` es anterior al lote; activa la fecha de reproceso acordada. |
| `allowed_null` / `rejected_null` | `phone` nulo aceptado; campo obligatorio nulo enviado a cuarentena. |
| `orphan_reference` | Pedido con cliente o producto inexistente; ORD-005. |
| `extra_json_field` | JSON válido con atributo desconocido; SCH-001/rescued data. |
| `malformed_json` | Línea no parseable, aislada del resto; SCH-002. |

## Aceptación Fase 0

1. Para un fixture pequeño se puede calcular manualmente `total_revenue_eur`, `order_count` y reembolsos; el resultado coincide con la definición anterior.
2. Cada escenario tiene regla, severidad, destino y aserción `unittest` previstos.
3. Los duplicados exactos son aceptados/ignorados lógicamente; conflicto de misma versión, referencias huérfanas y nulos requeridos no se silencian.
4. El contrato diferencia JSON adicional rescatable de registros no parseables en cuarentena.
