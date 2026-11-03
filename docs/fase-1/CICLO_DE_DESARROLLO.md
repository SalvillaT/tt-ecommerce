# Fase 1 — Ciclo de desarrollo y control de coste

**Estado:** decisión de diseño offline. No hay recursos desplegados, cómputo activo ni protecciones verificadas. Este runbook preventivo no autoriza credenciales, comandos de proveedor, `plan`, `apply` ni `destroy`: antes deben aprobarse los [gates](GATES_DESPLIEGUE.md).

## Decisión aceptada

Se conservarán entre sesiones el root `platform`, el workspace y ADLS. No se usará `terraform destroy` para detener cómputo ni `-target` como mecanismo operativo. El root `platform` también posee RG, ADLS/contenedores/rutas, Access Connector y RBAC; destruirlo puede eliminar datos y dependencias, no solo el workspace. El backend `bootstrap` es separado y su state no debe borrarse en un cierre de sesión.

Si se hubiera aplicado `databricks` tras sus gates, incluiría credencial/ubicación externa, el catálogo sintético y esquemas vacíos/grant escritor; sus propietarios y accesos efectivos, incluidos grants heredados, requieren revisión separada antes de cualquier retirada. El SQL Warehouse permanece opt-in desactivado en Fase 1 y se crea, si procede, en Fase 5 tras Gold y su gate de coste. No se cargará PII ni datos reales.

## Sesión futura (solo tras gates y despliegue autorizado)

**Antes de trabajar**

1. Revisar en el workspace los estados de compute, ejecuciones de jobs y SQL Warehouses; investigar cualquier recurso que siga en ejecución.
2. Usar *Jobs compute* nuevo por ejecución y confirmar que termina al finalizar el job. Si se usa compute all-purpose, configurar autoterminación y un límite de tamaño aprobado. La autoterminación responde a inactividad; no sustituye la revisión humana ([referencia oficial](https://learn.microsoft.com/azure/databricks/compute/configure#automatic-termination)).
3. En Fase 1 mantener SQL Warehouse desactivado: `enable_sql_warehouse = false` y no se crea. En Fase 5, tras Gold y autorización secundaria explícita (`phase5_sql_warehouse_approved = true`), confirmar permisos, cuota y precio exacto antes de habilitar el `PRO`/`2X-Small` (máximo un clúster, `auto_stop_mins = 10`); no inferirlo de Serverless.

**Al terminar una demo**

1. Esperar a que terminen los jobs y comprobar de nuevo compute y Warehouses. Terminar manualmente cualquier compute all-purpose aún activo.
2. Detener manualmente el SQL Warehouse tras la demo, incluso con auto-stop de 10 minutos; un Warehouse inactivo sigue acumulando cargos hasta detenerse ([referencia oficial](https://learn.microsoft.com/azure/databricks/compute/sql-warehouse/create#configure-sql-warehouse-settings)).
3. Revisar Azure Cost Analysis y la configuración de presupuesto/alertas cuando exista. La facturación puede retrasarse; las alertas son aviso, no un tope técnico de gasto.

## Límites de coste y retirada

ADLS y otros recursos persistentes pueden tener coste aunque no haya compute. No se afirma que un workspace inactivo tenga coste plano cero ni que el objetivo de 10–20 EUR/mes esté garantizado. En France Central se reevaluarán coste, cuota/Premium, presupuesto y alertas antes del despliegue; las referencias West Europe no aplican. El usuario informa de 200 EUR de crédito anunciados, con unos 17 días de vigencia restantes a 2026-11-03; saldo efectivo, elegibilidad de Databricks, cobertura y precios siguen sin verificar.

Una retirada excepcional exige inventario, owner, preservación de state y revisión de datos/UC. Nunca se improvisa con `terraform destroy` de `platform` o con `-target`; la decisión y orden de destrucción se revisan fuera de este ciclo.
