# Fase 1 — Arquitectura objetivo y decisiones

**Estado:** diseño offline; no autoriza `apply` ni declara recursos creados. Complementa la Fase 0 y deberá reconciliarse con [infra/README.md](../../infra/README.md) cuando esté disponible.

## Alcance y grafo mínimo

Región cloud decidida: **West Europe** (distinta de la zona de negocio `Europe/Madrid`). El grafo MVP pretende dejar una base gobernable sin crear cómputo de datos:

```text
bootstrap state (opcional e independiente)
  └─ backend Blob protegido  ← identidad Terraform

RG de entorno
  ├─ ADLS Gen2 (HNS) ← Access Connector / identidad de jobs
  ├─ Databricks Premium workspace
  │    └─ [prerrequisito] metastore Unity Catalog + admin asignado
  └─ Access Connector con managed identity
       └─ RBAC Azure acotado sobre ADLS

UC (solo tras gate administrativo y soporte confirmado)
  └─ storage credential → external location → objetos futuros Bronze/Silver/Gold
```

### Must-have IaC MVP

- Resource group, etiquetas y convenciones de nombre parametrizadas.
- ADLS Gen2 con HNS y rutas/filesystems de datos previstas: `landing`, `bronze`, `silver`, `quarantine`, `gold` y rutas técnicas futuras de checkpoint/schema. No crean tablas ni cargan datos; crear directorios no configura ACLs de directorio ni grants UC.
- Workspace Azure Databricks Premium y Access Connector con identidad administrada.
- Asignaciones Azure RBAC de mínimo privilegio, mediante IDs de principals/grupos existentes como variables.
- Patrón de estado: backend Blob protegido, separado del almacenamiento de datos, **si** se completa el bootstrap. No se configura automáticamente. Para experimentación estrictamente local puede existir estado local temporal no versionado; no es colaboración ni producción.

No crear por defecto clusters, jobs, pipelines, SQL Warehouse, Power BI, secretos ni objetos UC. Key Vault solo se incorpora si un flujo posterior exige un secreto que no pueda evitarse con identidad administrada/autenticación aprobada.

### Dependiente de cuenta o administrador

- Confirmación de suscripción, tenant, cuotas y elegibilidad de la oferta Free Trial para cada cargo de Databricks.
- Existencia/creación y asignación de metastore Unity Catalog por un account admin; no se presupone que Terraform pueda crearlo o asignarlo.
- Account admin, metastore admin, grupos Entra reales y propietarios de recursos.
- Decisión de red y, si procede, aprobación de private endpoints/DNS/VNet injection.
- Creación de UC storage credential/external location solo si los permisos y la cobertura del provider aprobados lo permiten.

### Fuera del MVP inicial

VNet injection, Private Link, private DNS, NAT Gateway, SIEM/logging ampliado, SQL Warehouse, cómputo de jobs, Key Vault sin necesidad concreta y políticas UC de máscara. Son decisiones posteriores con coste y/o dependencias administrativas.

## Identidad y acceso mínimo

| Identidad o grupo | Azure | Workspace / UC futuro | Límite explícito |
| --- | --- | --- | --- |
| Principal Terraform/CI | Crear/leer solo recursos y RBAC del alcance autorizado; escritura del backend state | Ninguno por defecto | No usa credenciales humanas ni obtiene acceso general a datos. |
| `data-engineers` | Sin acceso directo amplio a ADLS por defecto | Operación de Bronze/Silver cuando UC exista | Acceso sujeto a necesidad operativa. |
| Access Connector / identidad de jobs | RBAC actual acotado al contenedor `lakehouse`; no incluye `landing` | Acceso a external locations futuras | Separada de identidades humanas; no hay ACL por prefijo aún. |
| `customer-data-stewards` | Sin acceso directo por defecto | Subconjunto Silver justificado | Contacto original solo tras controles UC. |
| `silver-analysts-authorized` | Sin acceso directo | Vista Silver aprobada y enmascarada | Nunca `SELECT` general a Silver. |
| `bi-executives` | Sin acceso directo | Solo Gold publicado/SQL Warehouse futuro | Sin Bronze, Silver ni cuarentena. |

BI y analistas no reciben RBAC Azure explícito a los datos. Sin embargo, Bronze, Silver y cuarentena **no** quedan restringidos aún mediante ACLs de directorio ni grants UC: el conector tiene acceso al contenedor `lakehouse` y el control por prefijo no está implementado. El acceso futuro a `landing` para ingesta requerirá una identidad y una concesión separadas, de alcance mínimo. El enmascaramiento dinámico es una política UC futura y no concede `SELECT`. No versionar secretos, tokens, ficheros `tfstate` ni archivos con IDs sensibles; las variables reales se suministran fuera de Git.

## Red y exposición de almacenamiento

La configuración objetivo inicial deja el endpoint público de Storage y el workspace accesibles desde **cualquier red** para principals autenticados; no hay reglas de red/allowlist. El acceso anónimo a blobs está deshabilitado y se exige TLS, pero eso no equivale a red privada ni a aislamiento frente a Internet. Esta postura debe aprobarse explícitamente en G5 y solo es admisible para datos sintéticos: no se cargarán PII ni datos reales. Antes de ese uso, se elegirá y diseñará una alternativa de private networking o allowlist, incluido DNS, coste y operación.

## Coste: referencia y guardrails

El objetivo es 10–20 EUR/mes y el crédito Free Trial anunciado es aproximadamente 180 EUR, pero cobertura, caducidad y elegibilidad de cargos Databricks son **no verificadas**. No hay garantía de mantenerse bajo ese objetivo.

Referencias indicativas previamente verificadas para West Europe a 2026-11-02, que deben recalcularse en la calculadora retail antes de cualquier despliegue:

| Concepto hipotético | Referencia | Riesgo |
| --- | ---: | --- |
| Databricks Premium Jobs | 0.2693 EUR/DBU-h | El cargo depende de SKU/offer y del cómputo asociado. |
| VM D4s_v5 | 0.2065 EUR/h | Un nodo hipotético durante 1 h; no incluye todos los componentes. |
| SQL Serverless X-Small | 6 DBU/h × 0.8169 EUR/DBU-h = 4.9014 EUR/h | Ilustración de un SKU distinto; puede consumir el presupuesto rápidamente. |

El ejemplo Serverless X-Small **no es una estimación** del Warehouse opcional `PRO 2X-Small`: su precio DBU/VM debe revisarse específicamente antes de habilitarlo. En la primera aplicación permitida no habrá compute activo. Para fases posteriores: jobs efímeros, autoterminación, Warehouse solo opt-in con auto-stop, datos sintéticos pequeños y alertas de presupuesto. Un presupuesto alerta; no impone un tope técnico de gasto.

## Decisiones abiertas

- IDs de tenant/suscripción, nombres disponibles, etiquetas y contactos de alertas.
- Rol efectivo del principal Terraform y separación entre operador humano/CI.
- Metastore, account admin y modalidad autorizada de UC.
- Cuotas, elegibilidad Free Trial y cálculo retail vigente de cada SKU.
- Backend de estado definitivo, red permitida y momento de adoptar private networking.
- Volumen objetivo de infraestructura, retención, borrado y requisitos de seguridad/legal para datos no sintéticos.
