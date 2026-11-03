# Fase 1 — Guía de despliegue guiado

**Estado:** preparación offline lista; Fase 1 sigue **abierta**. Esta guía no autoriza comandos, autenticación, `plan`, `apply`, `destroy` ni llamadas cloud. Cada ejecución la realiza el usuario y requiere una autorización explícita posterior a la revisión del paso anterior.

## Qué está listo y qué no acredita

El 2026-11-03 el usuario validó offline los tres roots con Terraform temporal 1.13.5 Windows amd64 (SHA256 oficial). `platform` (azurerm 4.30.0) pasó fmt/init backend deshabilitado+lockfile readonly/validate tras sustituir el `resource_manager_id` deprecado por el ID del contenedor, sin avisos finales. `bootstrap` (azurerm 4.30.0) y `databricks` (provider 1.80.0) también pasaron fmt/init/validate con código 0. No hubo credenciales, Azure auth/API, state, plan/apply/destroy; `.terraform` quedó ignorado y lockfiles sin cambios. Esto acredita código local, no permisos, costes, propietarios ni acceso efectivo.

## Etapa 0 — Hechos de portal y decisiones humanas

Antes de ejecutar comandos Terraform que consulten Azure, el usuario comprueba en el portal y confirma sin compartir IDs, tokens, state ni secretos:

- oferta de suscripción, saldo/caducidad y elegibilidad Databricks;
- cuota disponible en France Central; coste persistente estimado de workspace/ADLS, presupuesto, alertas y owner;
- tenant, roles del principal aplicador, metastore UC existente compatible con France Central y asignable, DEFAULT managed storage, nombre visible y owner del grupo de cuenta escritor existente;
- elección de backend/state, acceso mínimo y recuperación;
- confirmación de que se mantiene la decisión G5 ya registrada: endpoint público sintético, sin allowlist; no es una verificación live.

La región prevista para `platform` y `bootstrap` opcional es France Central. West Europe mostró al usuario una advertencia de no admisión de nuevos clientes; que no aparezca en France Central no garantiza cuota, Free Trial ni Premium. G5 ya registra endpoints públicos sin allowlist, solo principals autenticados y datos sintéticos; TLS/RBAC/blob anónimo deshabilitado no son red privada ni cumplimiento GDPR, y Private Link/VNet queda diferido. No hace falta precio exacto de SQL PRO hasta Fase 5. Si falta cualquier otro punto, el resultado es **PAUSA**. Los tres booleanos UC son afirmaciones humanas, no tests; sin DEFAULT no se usa `storage_root` alternativo ni la external location como managed storage: se pausa y rediseña con UC admin.

El backend se separa de G5: contenedor privado `tfstate`, endpoint público sin allowlist solo para Entra/RBAC autenticado, Shared Key/usuarios locales deshabilitados, OAuth por defecto y 7 días de versionado/soft delete. `tfstate` es sensible (IDs y valores potencialmente sensibles), no PII por diseño y no es público por el endpoint; no es una afirmación de red privada/GDPR.

## Etapa 1 — Siguiente handoff: Portal manual

**Usuario:** antes de cualquier plan, inspecciona manualmente en Portal la cuota y elegibilidad de Databricks Premium en France Central, la oferta/crédito vigente y el coste persistente estimado de ADLS/workspace, presupuesto, alertas y owner. Consultar en modo lectura los estados de registro de `Microsoft.Storage`, `Microsoft.Authorization` y `Microsoft.Databricks`; no pulsar **Register**. El usuario reporta `Microsoft.Databricks` como Registered, pero debe confirmarse en la sesión autorizada. No usar Portal **Create**. Compartir solo un resumen redactado; si no se confirma, **PAUSA**.

El usuario reporta que creó un presupuesto mensual de suscripción de 50 EUR con alertas al 40% y 100%; antes de apply se confirma alcance, moneda y destinatarios/notificaciones. No se infiere ningún límite técnico de gasto.

## Etapa 2 — Preparación local del usuario

**Usuario:** ejecuta cualquier comando Terraform solo si lo decide y tras la revisión acordada; la validación previa ya cubre `platform`, `bootstrap` y `databricks`, pero no sustituye evidencia live. Para AzureRM 4.30.0, un plan requiere CLI login con rol Owner y `ARM_SUBSCRIPTION_ID` disponible solo en la sesión local (por ejemplo, asignar el placeholder `<subscription-id>` a esa variable en PowerShell). Nunca pegar, persistir o versionar el ID, credenciales, state o variables reales. Tras el cambio de provider previsto (`features {}` y `resource_provider_registrations = "none"` en platform/bootstrap), el usuario revalida localmente esos roots antes de cualquier plan. No usar Portal **Create** como sustituto de los roots Terraform.

**Pausa/revisión:** compartir únicamente un resumen redactado de resultados y cambios previstos. Si aparecen secretos, IDs, coste inesperado, una desviación de G5 o discrepancia de provider, parar.

## Etapa 3 — Inspección y plan por root

`resource_provider_registrations = "none"` impide que plan/apply registre automáticamente resource providers. Si alguno requerido no está Registered, el proceso debe **PAUSAR** o fallar hasta que un proceso aprobado lo resuelva; no se corrige pulsando Register durante esta guía.

El usuario reporta dos applies bootstrap exitosos: RG de estado, Storage Account Standard LRS y contenedor privado `tfstate`; después un role assignment `Storage Blob Data Contributor` en ese contenedor a la identidad Azure CLI actual. Bootstrap conserva estado local. El init autenticado inicial de platform mostró que faltaba `backend "azurerm" {}` y quedó en backend local; el usuario reporta que el backend remoto se configuró posteriormente antes de Stage 1. Esta guía no aporta evidencia cloud directa ni autoriza nuevos comandos.

El usuario reporta Stage 1 de `platform`, los probes ADLS Gen2 correctos y Stage 2 revisado/autorizado aplicado. Stage 2 creó exactamente siete rutas sin cambios ni destrucciones: `landing/orders`, `landing/customers`, `landing/products`, `lakehouse/bronze`, `lakehouse/silver`, `lakehouse/quarantine` y `lakehouse/gold`. Los flags `create_data_lake_directories` y `terraform_operator_blob_access_confirmed` permanecen `true` y nunca deben volver a `false`, pues destruirían las rutas. No usar `-target`; ningún plan o apply futuro queda implícitamente autorizado. Los grants del operador siguen siendo distintos del RBAC del Access Connector y no dan acceso a contenedores futuros. El root `databricks`/UC, datos, jobs y SQL siguen sin apply.

El usuario reporta metastore France Central asignado al workspace. Decisión UC aprobada: su root DEFAULT queda vacío y ningún plan lo muta o reemplaza. `tt_ecommerce_synthetic` usará managed storage de catálogo en un child path exclusivo bajo el storage dedicado Stage 3; `lakehouse` queda aislado para external location. Esto evita ForceNew del provider 1.80 sobre root de metastore existente y ambigüedad root/external location.

El usuario reporta Stage 3 autorizado de `platform`: storage privado dedicado para managed storage, un Access Connector SystemAssigned dedicado y un único grant Blob Data Contributor limitado a ese storage; no se documentan nombres/IDs. Stage 3 no configura UC. También reporta que la identidad cloud-only ya es Account Admin, Workspace Admin y único Metastore Admin mediante transferencia autorizada. No tiene roles Azure de suscripción reportados. No retirar su Global Administrator temporal hasta verificar acceso cloud/provider y recuperación administrativa. Después, verificar autenticación Terraform, aprobar grupo escritor/membresía y configurar parent external location/child path de catálogo; solo entonces validar, revisar y autorizar separadamente un plan del root `databricks`.

**Usuario:** ejecuta el plan de un solo root y comparte un resumen redactado: recursos a crear/cambiar, coste/riesgo, owners/grants esperados y bloqueos. No compartir state ni credenciales.

**Asistente:** revisa el resumen. **No hay apply implícito:** el usuario debe autorizar por separado cada apply. Si el plan amplía privilegios, crea SQL, no respeta red/coste o no permite identificar owner, el resultado es **PAUSA**.

## Etapa 4 — Apply y evidencia por root

Tras autorización separada, el usuario aplica un root y conserva evidencia redactada de coste, RBAC y objetos creados. El metastore/account-admin assignment puede requerir operación manual externa.

Para `databricks`, comprobar antes de datos: credencial/external location, `tt_ecommerce_synthetic`, esquemas vacíos `bronze`, `silver`, `quarantine`, `gold`; propietario de cada objeto; y grants efectivos. El IaC declara únicamente `USE_CATALOG` y `USE_SCHEMA`/`CREATE_TABLE` para el grupo escritor. No demuestra que sea el único acceso: herencias, grants preexistentes y owners pueden otorgar más.

Probar con identidades reales permitidas y no autorizadas. BI, stewardship y analistas deben fallar lecturas y escrituras. Una tabla inexistente no prueba denegación: para una prueba de lectura se necesita un objeto temporal **vacío y sintético**, compute transitorio autorizado, evidencia del resultado y retirada posterior. Si no se autoriza ese compute, registrar el test como pendiente y mantener Fase 1 abierta. El Access Connector/external location cubre el contenedor `lakehouse` completo, no zonas/prefijos.

## Etapa 5 — Cierre o recuperación

**Pasa Fase 1** solo cuando la evidencia autorizada confirma fundación, costes aceptados, UC/owners/acceso efectivo y pruebas negativas, sin datos cargados. Entonces Fase 2 puede cargar sintéticos. Máscaras quedan en Fase 4 y SQL Warehouse en Fase 5.

Ante apply parcial, bloqueo o state inconsistente: pausar, preservar state/evidencia, inventariar recursos y pedir revisión. No usar `-target` ni `destroy` como limpieza. Conservar workspace y ADLS; detener compute cuando exista según el [ciclo de desarrollo](CICLO_DE_DESARROLLO.md).

## Bloqueadores que debe aportar o confirmar el usuario

1. Hechos de Etapa 0, especialmente coste/cuota/red y owner de alertas.
2. Identidad aplicadora/owner aprobada y capacidad de UC account/metastore admin.
3. DEFAULT managed storage, grupo escritor existente y membresía estrecha verificables.
4. Elección/recuperación del backend y autorización explícita, paso a paso, para inspección, cada plan y cada apply.
