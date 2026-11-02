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

**Objetivo:** preparar una base mínima, auditable y costeable para el lakehouse.

**Estado: EN CURSO (solo código y validación offline; sin `terraform apply`).** [Arquitectura y decisiones](fase-1/ARQUITECTURA_Y_DECISIONES.md) · [gates previos al despliegue](fase-1/GATES_DESPLIEGUE.md). La fase no se cerrará hasta comprobar recursos, permisos y costes en un despliegue autorizado.

**Dependencias:** Fase 0; acceso administrativo aprobado a Azure y Databricks.

**Tareas unitarias**

1. Confirmar suscripción, región, convención de nombres, etiquetas, presupuesto/alertas y responsables de recursos.
2. Diseñar módulos Terraform para resource group, ADLS Gen2, configuraciones de acceso necesarias, Databricks y dependencias soportadas por el provider elegido.
3. Configurar backend remoto protegido para `tfstate`, con acceso mínimo y mecanismo de bloqueo si está disponible; para una prueba aislada, documentar estado local no versionado y su limitación.
4. Crear o mapear grupos reales del tenant y registrar propietario/proceso de alta-baja: `data-engineers`, `customer-data-stewards`, `silver-analysts-authorized` y `bi-executives` (nombres provisionales). Definir la matriz: ingenieros/stewards con necesidad justificada ven contacto original; analistas autorizados solo un subconjunto Silver mediante vista con contacto enmascarado; BI/dirección solo Gold.
5. Preparar identidades administradas/service principals y permisos mínimos para despliegue, jobs y acceso a almacenamiento, separando identidad humana y automatización.
6. Decidir y configurar Key Vault o el gestor corporativo de secretos si el patrón necesita secretos; comprobar que no se imprimen en logs ni se incluyen en variables versionadas.
7. Validar prerrequisitos de Unity Catalog: metastore, asociación de workspace, almacenamiento y patrón de external location/credential aplicable al entorno. Documentar cualquier paso manual administrativo.
8. Crear los catálogos/esquemas y privilegios base mínimos para las rutas y objetos que se crearán en Bronze/Silver, sin conceder acceso general a Silver; completar y probar las políticas de masking en Fase 4.
9. Crear SQL Warehouse con política de apagado y capacidad inicial conservadora, tras validar permisos y coste.

**Entregables:** diseño de módulos Terraform, guía de despliegue/rollback, inventario de identidades y grupos, decisión de backend y lista de prerrequisitos UC.

**Aceptación verificable:** `terraform plan` se revisa sin secretos; no hay `tfstate` ni credenciales en el repositorio; identidades de job y personas tienen permisos diferenciados; los privilegios base restringen Bronze/Silver/cuarentena desde su creación; se puede demostrar que `bi-executives` no obtiene acceso directo al almacenamiento o Silver; los pasos UC pendientes quedan explícitos.

---

### Fase 2 — Bronze incremental, recuperación y observabilidad básica

**Objetivo:** cargar origen sintético sin mutarlo y poder reanudar la ingestión sin duplicar resultados lógicos.

**Dependencias:** Fases 0–1; ADLS, workspace y ubicación gobernada configurados.

**Tareas unitarias**

1. Crear área de aterrizaje con rutas por fuente y fecha de llegada (pedidos JSON, clientes CSV y catálogo CSV); definir quién puede escribir y quién puede leer con mínimo privilegio.
2. Implementar Auto Loader con esquema explícito o ubicación de esquema, checkpoint persistente, metadatos de archivo/ingestión y `rescued data` para drift.
3. Escribir Bronze Delta append-only con identificador de lote, sistema fuente, ruta física de archivo, timestamps y columnas de auditoría; nunca sobrescribir el payload raw y aplicar desde su creación los privilegios base definidos en Fase 1.
4. Registrar métricas por ejecución: archivos descubiertos, filas leídas, filas rescatadas, filas escritas, ventana y estado.
5. Probar reinicio desde checkpoint y reproceso controlado con el mismo lote; definir la estrategia de deduplicación posterior sin borrar evidencia Bronze.
6. Simular una columna inesperada y una fila malformada; verificar alerta/registro y preservación para diagnóstico.

**Entregables:** notebook/job de ingestión, configuración de checkpoint/esquema, tabla Bronze, log de ejecución y runbook inicial.

**Aceptación verificable:** dos ejecuciones sucesivas sin nuevos archivos no incrementan Bronze; tras reinicio, el job continúa desde su checkpoint; ruta física y timestamp de ingestión son consultables; el drift no se pierde silenciosamente y queda visible en `rescued data`/métricas.

---

### Fase 3 — Silver, calidad, deduplicación y cuarentena

**Objetivo:** producir datos conformados y joinables con reglas explícitas, sin promover errores a consumidores.

**Dependencias:** Fase 2 y contratos aprobados de Fase 0.

**Tareas unitarias**

1. Implementar lectura incremental de Bronze y normalización de tipos, fechas, moneda, estados y códigos de categoría/región.
2. Definir clave de deduplicación de pedidos (`order_id` + `line_id`) y orden determinista por versión/evento e ingestión; aplicar `MERGE` idempotente y conservar trazabilidad hacia Bronze. Las claves de clientes y productos se fijarán en sus contratos.
3. Añadir `created_at`, `updated_at`, `deleted_at` (soft delete cuando aplique), `source_system`, identificador de lote y resultado de calidad a cada fila Silver.
4. Aplicar reglas de Fase 0 antes de publicar: separar válidas, inválidas y casos pendientes de revisión en cuarentena con código y detalle de fallo; mantener Silver y cuarentena restringidos desde su creación según la matriz de Fase 1.
5. Implementar controles de contrato y tests: esquema, nulabilidad, unicidad, referencias a clientes/productos, rango y frescura.
6. Generar métricas de calidad por lote y bloquear o marcar el refresco Gold cuando fallen reglas críticas.
7. Ejecutar casos de prueba para duplicado, corrección, soft delete, nulo permitido, nulo rechazado, referencia inexistente y llegada tardía.

**Entregables:** pipelines Silver/cuarentena, suite de pruebas, reporte de calidad y runbook de recuperación.

**Aceptación verificable:** reejecutar el mismo lote deja el mismo estado Silver; cada fila de cuarentena tiene origen y motivo; una regla crítica fallida impide la promoción a Gold; todas las tablas Silver contienen las auditorías definidas; joins de referencia aprobados pasan sus tests.

---

### Fase 4 — Unity Catalog, privilegios y verificación por identidad

**Objetivo:** aplicar gobernanza efectiva de datos, no solo un diseño de permisos.

**Dependencias:** Fase 1 y tablas de Fases 2–3.

**Tareas unitarias**

1. Validar y completar la propiedad, catálogos y esquemas creados como control base en Fase 1 para Bronze, Silver, cuarentena y Gold.
2. Completar los privilegios mínimos sobre los controles base de Fase 1: ingeniería para operación, stewardship con caso justificado para contacto original, `silver-analysts-authorized` solo sobre una vista/subconjunto Silver aprobado y `bi-executives` solo sobre Gold publicado.
3. Mantener Silver restringido; no conceder `SELECT` general a analistas. Publicar una vista específica para el subconjunto permitido y reservar las tablas base a los grupos operativos/autorizados.
4. Configurar política de enmascaramiento dinámico para email/teléfono con pertenencia a grupos reales: ingenieros/stewards autorizados ven el original; analistas autorizados reciben valor enmascarado. Mantener una ruta segura de mantenimiento de grupo.
5. Verificar con identidades reales de prueba separadas: positivo para ingeniero/steward autorizado (original), positivo para analista autorizado (vista y máscara), positivo para BI/dirección (Gold), y negativo para cada acceso no permitido a Bronze/Silver/cuarentena. Verificar además que la política de máscara no otorga `SELECT` a una identidad sin privilegio.
6. Revisar logs/auditoría disponibles y documentar cómo investigar un acceso o un cambio de privilegios.

**Entregables:** matriz de acceso implementada, políticas UC, evidencias de pruebas por identidad y procedimiento de revisión de permisos.

**Aceptación verificable:** `bi-executives` no puede consultar Bronze, Silver ni cuarentena y sí puede leer Gold publicado; una identidad de ingeniería/steward con autorización recibe email/teléfono originales; una identidad de `silver-analysts-authorized` solo puede leer la vista/subconjunto permitido y recibe contacto enmascarado; identidades no autorizadas fallan las consultas; el administrador no se usa como evidencia y ninguna máscara actúa como sustituto de `GRANT`/restricción de tablas.

---

### Fase 5 — Gold con criterios de publicación y consumo BI

**Objetivo:** entregar métricas estables, útiles y con exposición controlada.

**Dependencias:** Fases 3–4 y validación de métricas por negocio.

**Tareas unitarias**

1. Construir agregados Gold de ingresos/pedidos por fecha, categoría, región y estado, sin columnas PII previstas.
2. Refrescar una ventana de fechas explícita contra una instantánea completa de Silver; usar sobrescritura selectiva con restricción de predicado activa para que una fecha sin ventas elimine resultados antiguos sin tocar otras fechas.
3. Asociar a cada fila Gold un resultado/puntuación de calidad y timestamp de refresco; definir el tratamiento para datos incompletos.
4. Aplicar los criterios acordados de umbral, supresión o generalización y registrar las celdas no publicadas. Revisar que no se promete ausencia absoluta de riesgo de inferencia.
5. Crear vistas Gold de consumo y permisos para BI; validar consultas desde SQL Warehouse con límites y apagado configurados.
6. Construir un informe Power BI inicial con modo Import. Evaluar DirectQuery solo si los requisitos de frescura lo justifican y tras medir comportamiento/coste del SQL Warehouse.
7. Comparar resultados con un conjunto de control conocido: ventas completadas, reembolsos y días sin ventas.

**Entregables:** tablas/vistas Gold, definición de métricas, evidencia de refresco de ventana, consulta de validación e informe Power BI MVP.

**Aceptación verificable:** un reembolso o cancelación elimina/recalcula correctamente la métrica en la ventana afectada; una fecha vacía no conserva una fila de ingresos antigua; fechas fuera de la ventana no cambian; BI solo consulta los objetos Gold autorizados; el informe coincide con el conjunto de control.

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

1. Confirmar tenant, región **cloud** (distinta de la zona comercial `Europe/Madrid` ya acordada), presupuesto, capacidad administrativa y patrón de red permitido.
2. Elegir el backend remoto y el mecanismo de bloqueo de Terraform conforme a estándares del entorno.
3. Validar la configuración concreta de Unity Catalog y qué partes puede automatizar la versión de provider disponible.
4. Definir volumen objetivo y dimensionamiento de infraestructura; el SLA objetivo, ingresos, ventana de reproceso y umbral Gold ya se acordaron en Fase 0, pero no se han medido ni implementado.
5. Decidir si Power BI necesita DirectQuery; el punto de partida es Import por simplicidad y control de coste.
6. Definir políticas de retención/borrado y el responsable de aprobar cualquier uso futuro de datos no sintéticos.
