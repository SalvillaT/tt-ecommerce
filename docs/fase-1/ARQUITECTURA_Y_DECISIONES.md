# Fase 1 — Arquitectura objetivo y decisiones

**Estado:** diseño offline; no autoriza `apply` ni declara recursos creados. Se alinea con el root [infra/databricks](../../infra/README.md): no hay autenticación, plan ni evidencia live.

El ciclo preventivo acordado para un futuro entorno autorizado conserva `platform` (workspace y ADLS) entre sesiones y detiene solo compute activo; véase [Ciclo de desarrollo](CICLO_DE_DESARROLLO.md). No hay recursos desplegados ni activos ahora.

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
    └─ metastore asignado + identidad aplicadora → DEFAULT managed location → credential/external location → tt_ecommerce_synthetic → schemas vacíos + grant escritor
```

### Must-have IaC MVP

- Resource group, etiquetas y convenciones de nombre parametrizadas.
- ADLS Gen2 con HNS y rutas/filesystems de datos previstas: `landing`, `bronze`, `silver`, `quarantine`, `gold` y rutas técnicas futuras de checkpoint/schema. No crean tablas ni cargan datos; crear directorios no configura ACLs de directorio ni grants UC.
- Workspace Azure Databricks Premium y Access Connector con identidad administrada.
- Asignaciones Azure RBAC de mínimo privilegio, mediante IDs de principals/grupos existentes como variables.
- Patrón de estado: backend Blob protegido, separado del almacenamiento de datos, **si** se completa el bootstrap. No se configura automáticamente. Para experimentación estrictamente local puede existir estado local temporal no versionado; no es colaboración ni producción.

No crear por defecto clusters, jobs, pipelines, SQL Warehouse, Power BI ni secretos. Los objetos UC de Fase 1 sí están codificados, pero solo se crean tras tres booleanos default-false que son afirmaciones humanas, no pruebas automáticas: metastore/asignación e identidad aplicadora confirmados, ubicación administrada DEFAULT confirmada y un único grupo escritor existente aprobado. Key Vault solo se incorpora si un flujo posterior exige un secreto que no pueda evitarse con identidad administrada/autenticación aprobada.

### Dependiente de cuenta o administrador

- Confirmación de suscripción, tenant, cuotas y elegibilidad de la oferta Free Trial para cada cargo de Databricks.
- Existencia y asignación por un account admin de un metastore Unity Catalog compatible con France Central; Terraform no lo crea ni lo asigna.
- Account admin, metastore admin, grupos Entra reales y propietarios de recursos.
- Decisión de red y, si procede, aprobación de private endpoints/DNS/VNet injection.
- Provider Databricks 1.80.0 y permisos/soporte efectivos para crear los objetos UC; esto requiere verificación autorizada, no se infiere del código.
- Ubicación administrada DEFAULT aprobada para el metastore. El catálogo `tt_ecommerce_synthetic` omite `storage_root`; si no existe esa ubicación, no se aplica el root y se rediseña con el administrador UC. La external location de `lakehouse` no es sustituto de managed storage.

### Fuera del MVP inicial

VNet injection, Private Link, private DNS, NAT Gateway, SIEM/logging ampliado, cómputo de jobs, Key Vault sin necesidad concreta y políticas UC de máscara. SQL Warehouse no forma parte de Fase 1: queda como código opt-in desactivado y solo se considerará en Fase 5, tras Gold y su gate de coste. Son decisiones posteriores con coste y/o dependencias administrativas.

## Identidad y acceso mínimo

| Identidad o grupo | Azure | Workspace / UC futuro | Límite explícito |
| --- | --- | --- | --- |
| Principal Terraform/CI | Crear/leer solo recursos y RBAC del alcance autorizado; escritura del backend state | Ninguno por defecto | No usa credenciales humanas ni obtiene acceso general a datos. |
| Grupo escritor aprobado (uno, existente) | Sin acceso directo amplio a ADLS por defecto | Grants IaC: `USE_CATALOG`; `USE_SCHEMA`/`CREATE_TABLE` en los cuatro esquemas | El IaC no declara `SELECT`/`MODIFY`; no prueba ausencia de grants heredados ni poderes de propietario. |
| Access Connector / identidad de jobs | RBAC actual acotado al contenedor `lakehouse`; no incluye `landing` | Acceso a external locations futuras | Separada de identidades humanas; no hay ACL por prefijo aún. |
| Analistas, BI y stewardship | Sin acceso directo explícito | Ningún grant declarado por este IaC en Fase 1 | Vistas, acceso Gold y máscaras se diseñan en Fase 4/5; verificar herencias/owners antes de afirmar acceso efectivo. |

BI y analistas no reciben RBAC Azure explícito ni grants UC **declarados por este IaC** en Fase 1; ello no demuestra ausencia de accesos efectivos. Antes del apply se inventarían grants/propietarios del metastore, credential y external location existentes, y se aprueban identidad aplicadora/propietaria dedicada y membresía exacta del grupo. La Fase 1 no se cerrará hasta verificar después del apply metastore/asignación, ubicación DEFAULT, propietarios y acceso efectivo de catálogo, cuatro esquemas, credential y external location con identidades permitidas/no autorizadas; BI, stewardship y analistas deben fallar lecturas y escrituras, sin datos. El Access Connector y la external location cubren el contenedor `lakehouse` completo, no aíslan zonas/prefijos. El acceso futuro a `landing` requiere identidad y concesión separadas. Máscaras: Fase 4; no PII real. No versionar secretos, tokens, `tfstate` ni IDs sensibles.

## Red y exposición de almacenamiento

La configuración objetivo inicial deja el endpoint público de Storage y el workspace accesibles desde **cualquier red** para principals autenticados; no hay reglas de red/allowlist. El acceso anónimo a blobs está deshabilitado y se exige TLS, pero eso no equivale a red privada ni a aislamiento frente a Internet. Esta postura debe aprobarse explícitamente en G5 y solo es admisible para datos sintéticos: no se cargarán PII ni datos reales. Antes de ese uso, se elegirá y diseñará una alternativa de private networking o allowlist, incluido DNS, coste y operación.

## Coste: referencia y guardrails

El objetivo de 10–20 EUR/mes no está garantizado. A 2026-11-03 el usuario informa de 200 EUR de crédito promocional anunciados y unos 17 días restantes; saldo, caducidad efectiva, cobertura y elegibilidad Databricks siguen sin verificar. Antes de desplegar, reevaluar en France Central el coste persistente de ADLS/workspace, cuota/Premium, presupuesto, alertas y owner. Las cifras históricas de West Europe no se usan como estimaciones France Central. SQL PRO no requiere precio hasta Fase 5; entonces se revisará específicamente. En la primera aplicación permitida no habrá compute activo. Un presupuesto alerta; no impone un tope técnico de gasto.

La conservación del workspace no demuestra ausencia de coste: ADLS y otros recursos persistentes pueden facturar y no se afirma un cargo plano cero para un workspace inactivo. El cierre de sesión es operativo (comprobación y parada de compute/SQL), no un `terraform destroy` del root `platform`, que también contiene RG, ADLS, Access Connector y RBAC.

## Decisiones abiertas

- IDs de tenant/suscripción, nombres disponibles, etiquetas y contactos de alertas.
- Rol efectivo del principal Terraform y separación entre operador humano/CI.
- Metastore, account admin, modalidad autorizada de UC y soporte/permisos exactos de Terraform para storage credential, external location y grants.
- Cuotas, elegibilidad Free Trial/Premium y cálculo retail vigente en France Central.
- Backend de estado definitivo, red permitida y momento de adoptar private networking.
- Volumen objetivo de infraestructura, retención, borrado y requisitos de seguridad/legal para datos no sintéticos.
