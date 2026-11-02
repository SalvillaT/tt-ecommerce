# Fase 0 — Linaje, privacidad y accesos v1

**Navegación:** [contratos](CONTRATOS.md) · [métricas y calidad](METRICAS_Y_CALIDAD.md). Es una línea base de diseño: no afirma cumplimiento GDPR, anonimización ni permisos ya implantados.

## Linaje previsto

| Fuente / propietario | Bronze append-only | Silver o cuarentena | Gold / consumidor |
| --- | --- | --- | --- |
| `orders_<batch_id>.jsonl` — `ecommerce-operations` | Payload, nombre relativo de archivo, `batch_id`, checksum de manifiesto y metadatos de ingestión futuros. | Línea vigente por (`order_id`,`line_id`), tipada y validada; inválida a cuarentena con regla/razón/origen. | Ingresos, pedidos únicos y reembolsos por fecha de negocio, categoría, región y canal. |
| `customers_<batch_id>.csv` — `customer-operations` | Payload y procedencia de lote. | Maestro de cliente válido; error de contrato a cuarentena. | No se publica email/teléfono ni `customer_id`. |
| `products_<batch_id>.csv` — `catalog-operations` | Payload y procedencia de lote. | Maestro de producto válido; error a cuarentena. | Categoría, marca solo si pasa criterios de publicación. |
| `manifest_<batch_id>.json` — generador | Evidencia de reproducibilidad, filas y SHA-256. | Control de completitud y trazabilidad de lote. | No es fuente de métrica. |

La cuarentena debe retener el identificador de regla, clave disponible, `batch_id`, archivo/origen y payload o referencia segura al payload. Bronze conserva evidencia raw y no se transforma in situ. Silver no es una zona de consumo público. Gold se construye desde una instantánea Silver completa para la ventana solicitada; Gold no lee Bronze directamente.

## Datos personales y minimización

| Dato | Clasificación de diseño | Capa / tratamiento |
| --- | --- | --- |
| `email`, `phone` | PII sintética | Bronze/Silver restringidos; máscara dinámica en la vista Silver aprobada para analistas. Nunca en Gold. |
| `customer_id`, `order_id` | Identificador/pseudónimo de negocio | Restringido en Bronze/Silver; no publicado en Gold. |
| Región, fecha, categoría, canal, importe | Datos de negocio con posible riesgo de inferencia combinada | Gold solo tras control de tamaño de grupo y revisión de dimensiones. |

No se usarán contactos ni identidades reales en el generador. Si en el futuro se proponen hashes, un hash con salt secreto puede reducir exposición operativa, pero no equivale a anonimización; requerirá una decisión separada sobre gestión del secreto, rotación, acceso y riesgo de enlace. Retención, borrado, minimización operativa y auditoría son objetivos pendientes, no controles certificados.

### Baseline de publicación Gold acordado

La única cuadrícula publicable del MVP tiene grano (`business_date`, `category`, `region_code`, `channel`). Para cada celda se calcula **internamente** `privacy_subject_count = count(distinct customer_id)` sobre las líneas vigentes válidas en estado `COMPLETED`, `PARTIALLY_REFUNDED` o `REFUNDED` que contribuyen a alguna medida de esa celda (ingresos, pedidos o líneas reembolsadas). Se publican **todas** las medidas de la celda solo si `privacy_subject_count >= 5`; si no se alcanza, se suprime la celda entera. Ni `customer_id` ni el contador de sujetos se publican en Gold.

En el MVP no se publicarán simultáneamente totales, subtotales, vistas alternativas o roll-ups solapados que permitan reconstruir por resta una celda suprimida. Cada refresco reevalúa el umbral, pero comparar resultados entre refrescos o cruzarlos con datos externos aún puede permitir inferencias. Este criterio es una decisión de diseño F0, **no** está implementado en Databricks y no garantiza anonimización ni cumplimiento GDPR; el riesgo residual requiere revisión si cambian dimensiones, consumidores o datos.

## Matriz de acceso mínimo propuesta

Los grupos son nombres provisionales a mapear a identidades reales del tenant. Unity Catalog debe conceder privilegios de catálogo, esquema, tabla y vista por separado; una máscara no crea permiso `SELECT`.

| Grupo | Bronze | Silver base / cuarentena | Vista Silver autorizada | Gold / SQL Warehouse |
| --- | --- | --- | --- | --- |
| `data-engineers` | Operación necesaria | Operación necesaria; originales solo con necesidad justificada | No necesaria | Lectura/operación necesaria |
| `customer-data-stewards` | No por defecto | Subconjunto justificado, contacto original | Opcional | No por defecto |
| `silver-analysts-authorized` | No | No | `SELECT` mínimo; email/teléfono enmascarados | Solo si se aprueba caso de uso |
| `bi-executives` | No | No | No | Solo objetos Gold publicados, para Power BI/SQL Warehouse |

En Fase 1 se crean los límites base antes de crear/llenar Bronze, Silver o cuarentena. En Fase 4 se completan políticas de máscara y se prueban con identidades reales: ingeniero/steward autorizado ve originales, analista autorizado ve únicamente el subconjunto y valores enmascarados, BI solo Gold, y una identidad sin `SELECT` falla aunque exista la política de máscara. Identidades de job se separan de identidades humanas y reciben solo lo necesario para su tarea. Secretos y estado Terraform no se guardan en Git; Key Vault u otro gestor aprobado se decidirá al desplegar.

## Pendientes posteriores a Fase 0

| Pendiente | Cómo decidir/verificar |
| --- | --- |
| Tenant, región cloud, presupuesto, identidades y patrón de red | Confirmar con plataforma antes de Fase 1; la región cloud no está decidida por la zona comercial `Europe/Madrid`. |
| Volumen objetivo y dimensionamiento de infraestructura | Estimar carga/coste antes de desplegar; el fixture pequeño no es una previsión. |
| Backend Terraform y prerrequisitos/automatización UC | Confirmar el patrón aprobado y soporte del provider en el entorno Azure/Databricks elegido. |
| Retención, borrado, seguridad y requisitos legales | Acordar responsables y política antes de cualquier dato no sintético. |
| Nuevas dimensiones, consumidores o políticas de salida Gold | Revisar riesgo de inferencia, incluyendo diferencias entre celdas/refrescos, antes de ampliar la publicación. |

## Aceptación Fase 0

- Cada archivo fuente, clave y destino aparece en el linaje y es trazable mediante `batch_id` y checksum.
- La clasificación identifica explícitamente PII y excluye email, teléfono y claves de cliente de Gold.
- La matriz permite redactar pruebas positivas y negativas de acceso sin abrir Silver de forma general.
- Las decisiones F0 aprobadas están registradas en el [acta](CONTRATOS.md#acta-de-decisiones-f0-v1); los pendientes posteriores se separan de ellas. El documento no promete cumplimiento normativo ni ausencia absoluta de reidentificación.
