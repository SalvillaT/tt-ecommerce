# Fase 1 — Arquitectura objetivo y decisiones

**Estado:** Fase 1 cerrada según evidencia reportada por el usuario. El usuario reporta dos applies bootstrap, backend remoto funcional y Stage 1–3 autorizados de `platform`; Stage 3 añade storage/conector/RBAC dedicados. El metastore France Central está asignado y su root DEFAULT seguirá vacío por decisión aprobada. Bootstrap conserva estado local. Tras el apply UC remediado, el usuario reporta lecturas directas de solo lectura de los objetos UC base y grants, junto con acceso permitido a catálogo/esquemas y denegado a external location/credencial para una identidad escritora no administradora. Tras retirar el Global Administrator temporal de la administración cloud/UC, el perfil administrativo de Databricks siguió operativo y el plan posterior convergió sin cambios. El usuario reporta presupuesto mensual de suscripción de 50 EUR con alertas creado. Inventario reportado: 0 clusters, 0 jobs y un único SQL Warehouse preexistente detenido; Terraform no lo creó y lo mantiene desactivado. No hay datos sintéticos, jobs, compute ni SQL Warehouse iniciado por el proyecto. No hay evidencia cloud directa independiente en este repositorio. Las fases futuras siguen siendo separadas. Esta guía no autoriza nuevos comandos. Se alinea con el root [infra/databricks](../../infra/README.md).

El usuario reporta que se preservaron los roles Databricks requeridos tras retirar el Global Administrator Entra temporal de la administración cloud/UC. Databricks CLI v1.22.1 está instalado y un perfil local OAuth de workspace autenticó `current-user me` con salida 0. De forma separada se creó una identidad cloud-only no administradora, un grupo de ingeniería y su membresía; el grupo recibió exclusivamente Workspace access, sin consumer/SQL/admin, clusters sin restricciones ni pools. No se documentan identidades, credenciales ni roles Azure de suscripción.

**Evidencia offline (2026-11-03):** el usuario validó `platform`, `bootstrap` y `databricks` con Terraform temporal 1.13.5 Windows amd64 verificado. Los tres roots pasaron fmt/init con backend deshabilitado y lockfile readonly/validate; platform/bootstrap usaron azurerm 4.30.0 y databricks provider 1.80.0. En `platform` se sustituyó el `resource_manager_id` deprecado por el ID del contenedor y el validate final no emitió avisos. Esa evidencia valida código, no la arquitectura live. Posteriormente, el usuario reporta bootstrap, backend remoto de platform, Stage 1 y probes ADLS Gen2 de solo lectura correctos; este repositorio no contiene evidencia cloud directa.

El ciclo preventivo acordado conserva `platform` (workspace y ADLS) entre sesiones y detiene solo compute activo; véase [Ciclo de desarrollo](CICLO_DE_DESARROLLO.md). El reporte acredita solo los recursos y rutas indicados, no CI, UC ni controles live adicionales; UC no está desplegado.

## Alcance y grafo mínimo

Región prevista: **France Central** para resource group, ADLS, workspace Databricks, Access Connector y `bootstrap` opcional (distinta de la zona de negocio `Europe/Madrid`). El portal mostró al usuario una advertencia de no admisión de nuevos clientes en West Europe, ausente en France Central; no garantiza cuota, elegibilidad Free Trial ni cobertura Databricks Premium. El grafo MVP no crea cómputo de datos ni SQL Warehouse en Fase 1:

```text
bootstrap state (opcional e independiente)
   └─ backend Blob protegido en France Central  ← identidad Terraform

RG de entorno
  ├─ ADLS Gen2 (HNS) ← Access Connector / identidad de jobs
  ├─ Databricks Premium workspace
  │    └─ [prerrequisito] metastore Unity Catalog + admin asignado
  └─ Access Connector con managed identity
       └─ RBAC Azure acotado sobre ADLS

UC (solo tras tres gates default-false)
    └─ metastore asignado + identidad aplicadora → credential/external location + child path de catálogo → tt_ecommerce_synthetic → schemas vacíos + grant escritor
```

### Must-have IaC MVP

- Resource group, etiquetas y convenciones de nombre parametrizadas.
- El usuario reporta Stage 1 con ADLS Gen2 HNS y contenedores privados `landing`/`lakehouse`, y Stage 2 con siete rutas creadas: `landing/orders`, `landing/customers`, `landing/products`, `lakehouse/bronze`, `lakehouse/silver`, `lakehouse/quarantine` y `lakehouse/gold`. Los flags `create_data_lake_directories` y `terraform_operator_blob_access_confirmed` siguen en `true` y nunca deben volver a `false`, pues destruirían las rutas. Esto no concede ni demuestra acceso de CI o UC; futuros applies requieren revisión y autorización independientes.
- Workspace Azure Databricks Premium y Access Connector con identidad administrada.
- Asignaciones Azure RBAC de mínimo privilegio, mediante IDs de principals/grupos existentes como variables.
- Patrón de estado: backend Blob separado del lago de datos, con contenedor privado, OAuth/Entra-RBAC, Shared Key y usuarios locales deshabilitados, versionado y soft delete de blob/contenedor de 7 días. Su endpoint público sin allowlist es una excepción separada para principals autenticados; `tfstate` es sensible (IDs/valores de recurso potencialmente sensibles), no PII por diseño, y no se vuelve público por el endpoint. El usuario reporta RG de estado, Storage Standard LRS, contenedor `tfstate` y role assignment `Storage Blob Data Contributor` al principal Azure CLI actual, todos aplicados. Bootstrap conserva estado local. El init inicial de platform quedó local por no tener bloque backend; el usuario reporta que el backend remoto se configuró antes de Stage 1. El role de state no da Owner-wide ni acceso al lago; Owner de suscripción no equivale a acceso Entra al plano de datos Blob.

No crear por defecto clusters, jobs, pipelines, SQL Warehouse, Power BI ni secretos. Los objetos UC de Fase 1 sí están codificados, pero solo se crean tras tres booleanos default-false que son afirmaciones humanas, no pruebas automáticas: metastore/asignación e identidad aplicadora confirmados, parent external location y child path de catálogo aprobados, y un único grupo escritor existente aprobado. Key Vault solo se incorpora si un flujo posterior exige un secreto que no pueda evitarse con identidad administrada/autenticación aprobada.

### Dependiente de cuenta o administrador

- Confirmación de suscripción, tenant, cuotas y elegibilidad de la oferta Free Trial para cada cargo de Databricks.
- El usuario reporta metastore Unity Catalog compatible con France Central y asignado al workspace; Terraform no lo crea ni lo asigna. Su DEFAULT sigue vacío. Stage 3 aporta storage y conector dedicados, pero no configura el root ni acredita autoridad UC. La evidencia reportada cubre lecturas directas, grants efectivos, prueba positiva/negativa del grupo escritor, presupuesto/alertas, recuperación administrativa y plan final sin cambios. No usar cleanup, `destroy` ni `-target`.
- Account admin, metastore admin, grupos Entra reales y propietarios de recursos.
- Private Link, private DNS y VNet injection quedan diferidos a un diseño posterior; no son requisito de la postura sintética aprobada de Fase 1.
- Provider Databricks 1.80.0 y permisos/soporte efectivos para crear los objetos UC; esto requiere verificación autorizada, no se infiere del código.
- AzureRM 4.30.0 requerirá `features {}` y `resource_provider_registrations = "none"` en platform/bootstrap. El plan necesita Azure CLI login con Owner y `ARM_SUBSCRIPTION_ID` local; el ID nunca se comparte ni versiona. `none` evita el registro automático de providers: comprobar en lectura `Microsoft.Storage`, `Microsoft.Authorization` y `Microsoft.Databricks` antes del plan; el usuario reporta solo este último como Registered.
- El root DEFAULT del metastore existente permanece vacío y ningún plan lo muta/reemplaza. `tt_ecommerce_synthetic` tendrá managed storage a nivel de catálogo, en un child path exclusivo bajo el storage dedicado de Stage 3. `lakehouse` permanece aislado para external location. Esta opción evita el riesgo ForceNew del provider 1.80 sobre root de metastore existente y ambigüedad root/external location.

### Fuera del MVP inicial

VNet injection, Private Link, private DNS, NAT Gateway, SIEM/logging ampliado, cómputo de jobs, Key Vault sin necesidad concreta y políticas UC de máscara. SQL Warehouse no forma parte de Fase 1: queda como código opt-in desactivado y solo se considerará en Fase 5, tras Gold y su gate de coste. Son decisiones posteriores con coste y/o dependencias administrativas.

## Identidad y acceso mínimo

| Identidad o grupo | Azure | Workspace / UC futuro | Límite explícito |
| --- | --- | --- | --- |
| Principal Terraform/CI | Crear/leer solo recursos y RBAC del alcance autorizado; escritura del backend state | Ninguno por defecto | No usa credenciales humanas ni obtiene acceso general a datos. |
| Grupo escritor aprobado (uno, existente) | Sin acceso directo amplio a ADLS por defecto | Según reporte, solo Workspace access actual; tras apply, grants IaC: `USE_CATALOG`; `USE_SCHEMA`/`CREATE_TABLE` en los cuatro esquemas | Sin consumer/SQL/admin, clusters sin restricciones ni pools según reporte. El IaC no declara `SELECT`/`MODIFY`; no prueba ausencia de grants heredados ni poderes de propietario. |
| Operador Azure CLI actual | Dos grants IaC Blob Data Contributor: solo `landing` y `lakehouse` | Ninguno por defecto | Para rutas; sin acceso a futuros contenedores y distinto del connector. |
| Access Connector / identidad de jobs | RBAC actual acotado al contenedor `lakehouse`; no incluye `landing` | Acceso a external locations futuras | Separada de la identidad operadora; no hay ACL por prefijo aún. |
| Analistas, BI y stewardship | Sin acceso directo explícito | Ningún grant declarado por este IaC en Fase 1 | Vistas, acceso Gold y máscaras se diseñan en Fase 4/5; verificar herencias/owners antes de afirmar acceso efectivo. |

BI y analistas no reciben RBAC Azure explícito ni grants UC **declarados por este IaC** en Fase 1; ello no demuestra ausencia de accesos efectivos. Antes del apply se inventarían grants/propietarios del metastore, credential y external location existentes, y se aprueban identidad aplicadora/propietaria dedicada y membresía exacta del grupo. La Fase 1 no se cerrará hasta verificar después del apply metastore/asignación, el parent external location y child path de catálogo aprobados, propietarios y acceso efectivo de catálogo, cuatro esquemas, credential y external location con identidades permitidas/no autorizadas; BI, stewardship y analistas deben fallar lecturas y escrituras, sin datos. El Access Connector y la external location cubren el contenedor `lakehouse` completo, no aíslan zonas/prefijos. El acceso futuro a `landing` requiere identidad y concesión separadas. Máscaras: Fase 4; no PII real. No versionar secretos, tokens, `tfstate` ni IDs sensibles.

## Red y exposición de almacenamiento

**Decisión G5 registrada:** Storage y workspace usarán endpoints públicos, alcanzables desde **cualquier red** solo por principals autenticados, sin reglas de red/allowlist, para este proyecto sintético. El acceso anónimo a blobs está deshabilitado y se exige TLS, pero TLS, RBAC y ausencia de blobs anónimos no equivalen a red privada, aislamiento frente a Internet ni cumplimiento GDPR. No se cargarán PII ni datos reales. Private Link/VNet/private DNS quedan diferidos a un diseño, coste y operación separados. Esta decisión no declara que endpoints ni controles estén desplegados o verificados.

## Coste: referencia y guardrails

El objetivo de 10–20 EUR/mes no está garantizado. A 2026-11-03 el usuario informa de 200 EUR de crédito promocional anunciados y unos 17 días restantes; saldo, caducidad efectiva, cobertura y elegibilidad Databricks siguen sin verificar. Antes de desplegar, reevaluar en France Central el coste persistente de ADLS/workspace, cuota/Premium, presupuesto, alertas y owner. Las cifras históricas de West Europe no se usan como estimaciones France Central. SQL PRO no requiere precio hasta Fase 5; entonces se revisará específicamente. En la primera aplicación permitida no habrá compute activo. Un presupuesto alerta; no impone un tope técnico de gasto.

La conservación del workspace no demuestra ausencia de coste: ADLS y otros recursos persistentes pueden facturar y no se afirma un cargo plano cero para un workspace inactivo. El cierre de sesión es operativo (comprobación y parada de compute/SQL), no un `terraform destroy` del root `platform`, que también contiene RG, ADLS, Access Connector y RBAC.

## Decisiones abiertas

- IDs de tenant/suscripción, nombres disponibles, etiquetas y contactos de alertas.
- Rol efectivo del principal Terraform y separación entre operador humano/CI.
- Tfvars locales ignorados y selección de autenticación Terraform; validación estática del código actualizado, parent external location y child path de catálogo aprobados, confirmación de membresía/grants efectivos del grupo escritor, revisión del plan y autorización/aplicación separada; modalidad UC y soporte/permisos exactos de Terraform para credential, external location y grants.
- Cuotas, elegibilidad Free Trial/Premium y cálculo retail vigente en France Central.
- Alcance, moneda y destinatarios del presupuesto mensual que el usuario reporta como 50 EUR con alertas 40%/100%.
- Gates UC, evidencia de coste/uso y momento/diseño de private networking posterior.
- Volumen objetivo de infraestructura, retención, borrado y requisitos de seguridad/legal para datos no sintéticos.
