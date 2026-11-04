# Fase 0 — Contratos de datos v1

**Navegación:** [métricas y calidad](METRICAS_Y_CALIDAD.md) · [linaje y privacidad](LINAJE_Y_PRIVACIDAD.md). Esta es una especificación de fuentes sintéticas para el MVP; no declara recursos Azure ni pipelines implementados.

## Acta de decisiones F0 v1

**Aprobada por owner del proyecto el 2026-11-02.** Quedan fijados para el MVP: moneda `EUR`; zona comercial `Europe/Madrid`; lote diario antes de las 08:00 y disponibilidad objetivo antes de las 09:00; ingresos netos según la versión vigente y los estados de este contrato; reproceso de los últimos 7 días más las fechas de negocio afectadas; y publicación Gold solo en celdas canónicas con al menos 5 `customer_id` distintos, sin subtotales ni roll-ups solapados. El fixture pequeño es evidencia de prueba, no una estimación de volumen de infraestructura.

El SLA es un **objetivo futuro no medido**. La política Gold es una decisión de diseño de Fase 0, **no** está implementada en Databricks y no garantiza anonimización. La Fase 0 se cierra con el fixture `DEMO-001` (semilla 42, checksums en su manifiesto), CLI y 42 tests locales correctos mediante `py -m unittest discover -s tests -v`; esta evidencia no acredita despliegues Azure, Gold, Unity Catalog ni cumplimiento normativo.

## Convenciones comunes

| Aspecto | Contrato v1 |
| --- | --- |
| Owner lógico | Cada fuente identifica su responsable en la sección correspondiente; Ingeniería de datos custodia la ingestión y los contratos. |
| Codificación/formato | UTF-8 sin BOM y salto de línea `LF`. JSONL: un objeto JSON por línea. CSV: cabecera, RFC 4180, separador `,`, comillas `"`. |
| Fechas | `*_at`: RFC 3339 en UTC con sufijo `Z`; `business_date`: `YYYY-MM-DD`, fecha de negocio en la zona comercial acordada `Europe/Madrid`. |
| Importes | Cadenas decimales de dos posiciones, sin separador de miles ni coma decimal (`"19.90"`); moneda MVP `EUR`. No usar `float` para generar o comparar importes. |
| Identificadores | Strings no vacíos, sensibles a mayúsculas/minúsculas, patrón recomendado `[A-Za-z0-9_-]+`. |
| Auditoría de fuente | Todo registro versionado lleva `record_version` entero positivo y `event_at`/`updated_at`. El sistema añadirá auditoría de ingestión posteriormente; no forma parte del archivo fuente. |
| Frecuencia/SLA | Lote diario antes de las 08:00 de la zona comercial; disponibilidad objetivo antes de las 09:00. Es un SLA objetivo futuro, aún no medido. El volumen objetivo de infraestructura se decidirá antes de desplegar. |

Los cambios aditivos son compatibles si se documentan: Auto Loader podrá preservarlos como datos rescatados hasta que el contrato los adopte. Renombrar/eliminar campos, cambiar tipo/unidad/semántica, alterar una clave o añadir un valor de enumeración es **breaking**: requiere versión de contrato y aprobación. Un campo JSON extra no habilita por sí solo su uso en Silver/Gold.

## `orders` — JSONL versionado

- **Archivo:** `orders_<batch_id>.jsonl`.
- **Grano:** una versión/evento de una línea de pedido. Clave de negocio de línea: (`order_id`, `line_id`). Una misma línea puede aparecer en varios lotes con `record_version` creciente.
- **Owner lógico:** `ecommerce-operations`.

| Campo | Tipo / nulabilidad | Regla v1 |
| --- | --- | --- |
| `order_id`, `line_id` | string, no nulo | Clave compuesta de línea. |
| `record_version` | integer, no nulo | `>=1`; estrictamente creciente para una línea. |
| `event_at` | timestamp UTC, no nulo | Momento de la versión; no sustituye `business_date`. |
| `business_date` | date, no nulo | Fecha comercial de la venta original; no cambia por un reembolso posterior. |
| `customer_id`, `product_id` | string, no nulo | Referencias a archivos maestros. |
| `status` | string, no nulo | Enum definido abajo. |
| `quantity` | integer, no nulo | Unidades originalmente vendidas; `>0`. |
| `unit_price_eur`, `gross_amount_eur`, `refund_amount_eur`, `net_revenue_eur` | decimal(18,2) como string, no nulo | EUR; relaciones aritméticas definidas abajo. |
| `currency` | string, no nulo | Exactamente `EUR` en MVP. |
| `channel` | string, no nulo | `web` o `marketplace` en v1. |
| `region_code` | string, no nulo | Código de región comercial; catálogo inicial a validar. |
| `note` | string, nulo permitido | Máx. 200 caracteres; no debe contener PII real. |

`gross_amount_eur = quantity × unit_price_eur` en todos los estados, con igualdad exacta a dos decimales. En `PENDING` y `CANCELLED`, `refund_amount_eur = net_revenue_eur = 0`: el bruto representa el valor potencial de la línea, **no** una venta realizada. Solo para `COMPLETED`, `PARTIALLY_REFUNDED` y `REFUNDED` se aplica `net_revenue_eur = gross_amount_eur − refund_amount_eur`. En `COMPLETED`, `refund_amount_eur = 0`; en `REFUNDED`, `refund_amount_eur = gross_amount_eur` y neto `0`; en `PARTIALLY_REFUNDED`, `0 < refund_amount_eur < gross_amount_eur`.

Una primera versión puede registrar `PENDING` o `COMPLETED` (si el origen informa de la venta ya confirmada). Transiciones posteriores permitidas: `PENDING → COMPLETED | CANCELLED`; `COMPLETED → PARTIALLY_REFUNDED | REFUNDED`; `PARTIALLY_REFUNDED → PARTIALLY_REFUNDED | REFUNDED`. Una corrección puede emitir una versión posterior con el mismo estado y valores corregidos. No se acepta `COMPLETED → CANCELLED`: después de completado se expresa con reembolso. Una anulación total de un pedido se representa cancelando todas sus líneas pendientes; una devolución total se representa reembolsando todas sus líneas completadas. Una devolución parcial es una versión `PARTIALLY_REFUNDED` de la línea afectada, con su importe neto restante. Así Gold nunca suma la venta y el reembolso como hechos independientes: solo toma la versión vigente de cada línea.

Ejemplo sintético (dos versiones de la misma línea):

```json
{"order_id":"ORD-1001","line_id":"1","record_version":1,"event_at":"2026-01-10T10:00:00Z","business_date":"2026-01-10","customer_id":"CUS-001","product_id":"PRD-PALA-01","status":"COMPLETED","quantity":2,"unit_price_eur":"30.00","gross_amount_eur":"60.00","refund_amount_eur":"0.00","net_revenue_eur":"60.00","currency":"EUR","channel":"web","region_code":"ES-MD","note":null}
{"order_id":"ORD-1001","line_id":"1","record_version":2,"event_at":"2026-01-13T09:00:00Z","business_date":"2026-01-10","customer_id":"CUS-001","product_id":"PRD-PALA-01","status":"PARTIALLY_REFUNDED","quantity":2,"unit_price_eur":"30.00","gross_amount_eur":"60.00","refund_amount_eur":"30.00","net_revenue_eur":"30.00","currency":"EUR","channel":"web","region_code":"ES-MD","note":"synthetic return"}
```

## `customers` — CSV versionado

- **Archivo:** `customers_<batch_id>.csv`; cada fila es la versión vigente enviada para un cliente en ese lote, no un histórico SCD completo.
- **Grano/clave:** cliente, `customer_id`; la versión resuelve correcciones de maestro.
- **Owner lógico:** `customer-operations`.

| Campo | Tipo / nulabilidad | Regla v1 |
| --- | --- | --- |
| `customer_id` | string, no nulo | Clave. |
| `record_version` | integer, no nulo | `>=1`, creciente por cliente. |
| `updated_at` | timestamp UTC, no nulo | Marca de la versión. |
| `region_code` | string, no nulo | Región comercial. |
| `email` | string, no nulo | Sintético, formato básico válido; PII simulada. |
| `phone` | string, nulo permitido | Marcador sintético con forma `+34...`, no asignable a una línea real; no usar teléfonos potencialmente existentes. |
| `deleted_at` | timestamp UTC, nulo permitido | Soft delete; si existe, no se usa para nuevos pedidos. |

Ejemplo: `CUS-001,1,2026-01-01T08:00:00Z,ES-MD,ana.001@example.test,+34000000001,`. El prefijo nacional `0` de estos marcadores los hace no válidos como abonados españoles; se usan únicamente para ensayar máscaras, no como contactos reales.

## `products` — CSV versionado

- **Archivo:** `products_<batch_id>.csv`; cada fila es la versión vigente enviada para un producto en ese lote.
- **Grano/clave:** producto, `product_id`.
- **Owner lógico:** `catalog-operations`.

| Campo | Tipo / nulabilidad | Regla v1 |
| --- | --- | --- |
| `product_id` | string, no nulo | Clave. |
| `record_version` | integer, no nulo | `>=1`, creciente por producto. |
| `updated_at` | timestamp UTC, no nulo | Marca de la versión. |
| `name`, `brand`, `category` | string, no nulo | Categorías iniciales: `paddle`, `rubber`, `ball`, `table`, `net`, `accessory`. |
| `list_price_eur` | decimal(18,2) como string, no nulo | `>=0`, precio de catálogo, no sustituye precio histórico de pedido. |
| `currency` | string, no nulo | `EUR`. |
| `active` | boolean, no nulo | Producto vendible en catálogo; no invalida pedidos históricos. |

Ejemplo: `PRD-PALA-01,1,2026-01-01T08:00:00Z,Control Pro,TT Demo,paddle,30.00,EUR,true`

## Idempotencia, tardíos y manifiesto

La ingesta debe tratar un archivo con el mismo `batch_id` y SHA-256 como el mismo lote: reejecutarlo no debe producir nuevas versiones lógicas en Silver ni métricas Gold duplicadas. En Silver se conserva para cada clave la versión de mayor `record_version`. Dos payloads distintos con igual clave y versión son conflicto y se envían a cuarentena; no se desempatan por hora de llegada. Un evento con versión menor es histórico/no vigente: puede preservarse en Bronze, pero no reemplaza Silver.

Un evento tardío es válido si cumple contrato aunque llegue tras su `business_date`. La decisión F0 de reproceso es recalcular en cada ejecución Gold `[fecha_actual − 7 días, fecha_actual + 1 día)` y además las fechas `business_date` de líneas modificadas detectadas. La fecha de negocio de un refund sigue siendo la fecha original de la línea para que su sustitución corrija el agregado de venta. Es una especificación para fases posteriores, no un proceso implementado.

Cada lote incluye `manifest_<batch_id>.json`, sin rutas absolutas ni timestamps de reloj. Debe contener: `contract_version` (`"v1"`), `generator_version`, `batch_id`, `seed`, `scenarios`, `files` (nombre relativo, filas y `sha256` de bytes), y `total_rows`. Con igual versión de generador, `seed` y `batch_id`, el generador Python 3 estándar debe emitir bytes idénticos, incluido orden de filas, serialización, saltos de línea y manifiesto. La salida local prevista es `data/generated/<batch_id>/`; el ejemplo versionado está en `data/sample/DEMO-001/`. No se requieren dependencias de terceros: CLI y pruebas usan `argparse`, `json`, `csv`, `hashlib`, `random` y `unittest` u otros módulos estándar.

## Criterios de aceptación de contrato

- El generador acepta `--seed` y `--batch-id`; dos ejecuciones iguales generan los mismos checksums y manifiesto.
- Cada archivo cumple codificación, cabecera/JSONL, campos y tipos de v1; los escenarios negativos se emiten de forma deliberada y se identifican en el manifiesto.
- Una corrección, reembolso parcial, devolución total y anulación total tienen versiones y resultados de neto coherentes con este documento.
- Las pruebas locales derivan sin ambigüedad la fila vigente por clave y el resultado esperado del diseño Gold, sin implicar un pipeline desplegado.
