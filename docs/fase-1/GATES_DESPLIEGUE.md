# Fase 1 — Gates de despliegue y validación offline

**Estado:** Fase 1 no está cerrada. Este documento define qué debe comprobarse antes de solicitar permiso de despliegue; durante su preparación solo se permite código, documentación y validación offline. La secuencia operativa humana está en la [guía de despliegue guiado](GUIA_DESPLIEGUE_GUIADO.md); no autoriza operaciones por sí misma.

## Secuencia con gates

| Gate | Decisión/evidencia necesaria | Resultado si no pasa |
| --- | --- | --- |
| G0 — alcance | Confirmar entorno sintético, France Central y prohibición de `apply`. La advertencia portal de West Europe no prueba disponibilidad France Central. | No se inicializa ningún recurso cloud. |
| G1 — coste/oferta | Confirmar oferta/crédito/saldo/caducidad, elegibilidad Databricks Premium, cuota France Central y coste actual de ADLS/workspace, presupuesto y alertas. | No se habilita despliegue; SQL/compute quedan fuera. |
| G2 — identidad | Tenant, suscripción, principal Terraform/CI, grupos Entra, owners y roles efectivos aprobados. | No se codifican IDs reales ni grants definitivos. |
| G3 — estado | Decidir y autorizar bootstrap de backend Blob protegido, o estado local temporal; acceso mínimo, recuperación y responsable documentados. El backend no se configura automáticamente. | No se usa backend remoto. |
| G4 — UC | Las tres variables default-false se habilitan solo tras confirmar metastore asignado/identidad aplicadora, DEFAULT managed location y un único grupo escritor existente aprobado; verificar soporte/permisos del provider 1.80.0. | No se aplica el root Databricks ni se cargan datos. Sin DEFAULT, bloquear y rediseñar con UC admin. |
| G5 — red | Aprobar expresamente el endpoint público de Storage/workspace alcanzable desde cualquier red por principals autenticados, sin reglas de red, **solo para sintético**; o elegir rediseño private/allowlist y documentar DNS, coste y acceso. | No se realiza aplicación ni se cargan datos reales. |
| G6 — revisión | Revisión humana de coste, RBAC, nombres, tags y rollback; permiso explícito para una futura aplicación. | Se conserva trabajo offline. |

## Checklist previo a una futura aplicación

- [ ] Oferta exacta, crédito, vencimiento y cobertura de Databricks confirmados por el usuario/portal; presupuesto y contactos de alertas configurados o aprobados.
- [ ] Coste persistente y cuota de France Central revisados para ADLS/workspace; presupuesto, alertas y owner aprobados. La señal observada en West Europe no garantiza France Central; no reutilizar cifras West Europe. SQL PRO se revisa solo en Fase 5.
- [ ] Tenant/subscription, región cloud, cuota Databricks, roles y owners confirmados; ningún ID, token o secreto se añade a Git.
- [ ] Metastore UC compatible con France Central, asignación al workspace e identidad aplicadora con autoridad de objeto confirmados (`unity_catalog_prerequisites_confirmed`); Terraform no crea/asigna el metastore.
- [ ] Ubicación administrada DEFAULT aprobada para el metastore (`metastore_default_managed_storage_confirmed`). El catálogo omite `storage_root`; no usar la external location de `lakehouse` como managed storage. Si falta DEFAULT, bloquear y rediseñar con UC admin.
- [ ] Nombre no versionado de un único grupo escritor existente aprobado (`approved_engineer_writer_group_confirmed`); no usar `account users` ni comodines. El grant se limita a `USE_CATALOG` y, por esquema, `USE_SCHEMA`/`CREATE_TABLE`.
- [ ] Tratar los tres booleanos como afirmaciones humanas, no como tests: inventariar grants/propietarios preexistentes del metastore, credential y external location; aprobar identidad aplicadora/propietaria dedicada y membresía exacta del grupo antes del apply.
- [ ] Soporte y permisos efectivos del provider Databricks 1.80.0 para credencial, external location, catálogo, esquemas y grants verificados; no hay evidencia de ello hasta un despliegue autorizado.
- [ ] Patrón de red aprobado: se acepta expresamente que Storage y workspace públicos son alcanzables desde cualquier red por principals autenticados, sin allowlist, solo para sintético; o se aprueba un rediseño private/allowlist. TLS y ausencia de acceso anónimo no equivalen a red privada.
- [ ] Backend de estado aislado de ADLS de datos, con acceso mínimo y plan de recuperación; `tfstate` sigue fuera de Git.
- [ ] No se usarán identificadores/contactos reales en esta fase; retención, borrado y requisitos legales siguen pendientes.
- [ ] SQL Warehouse, clusters y jobs están deshabilitados por defecto. El Warehouse sigue como código opt-in `false` y no se crea en Fase 1; se reconsidera en Fase 5, con Gold existente, precio/cuota/permisos confirmados y auto-stop antes de Power BI.
- [ ] Si se autoriza compute, owner y runbook de [ciclo de desarrollo](CICLO_DE_DESARROLLO.md) revisados: Jobs compute por ejecución, autoterminación para all-purpose, comprobación de estados y parada manual de SQL tras demos.
- [ ] Se acepta que `platform` se conserva entre sesiones y que ADLS/recursos persistentes pueden tener coste; presupuesto/alertas no son límite duro. No se empleará `terraform destroy` ni `-target` para parar compute.

## Validación permitida ahora

Sin credenciales ni llamadas cloud: el 2026-11-03 se ejecutaron, solo en `infra/databricks`, `terraform fmt -check -recursive`, `terraform init -backend=false -lockfile=readonly` y `terraform validate`, con Terraform 1.13.5 temporal verificado por SHA256 oficial y provider Databricks 1.80.0; todos finalizaron con código 0. `git diff --check` quedó limpio. No hubo credenciales, Azure, state ni plan, y otros roots no se validaron en esta ejecución. No ejecutar `plan` contra la suscripción, `apply`, creación de metastore, asignaciones reales de RBAC ni carga de datos.

Criterios de revisión offline:

- La configuración describe solo el grafo MVP y los componentes opcionales están explícitamente desactivados o aislados.
- No hay secretos, `tfstate`, credenciales cloud ni valores reales sensibles en archivos versionados.
- ADLS declara HNS, transporte seguro y ausencia de acceso anónimo; se documenta que el endpoint público no tiene reglas de red y RBAC está limitado a principals parametrizados.
- BI, analistas y stewardship no reciben RBAC Azure explícito ni grants UC declarados por este IaC en Fase 1. Eso no revoca grants heredados/preexistentes ni poderes de propietario/creador. Antes de cargar datos, tras apply autorizado se verifican propietario y acceso efectivo del catálogo, cuatro esquemas, credential y external location, con negativas de lectura/escritura para esas identidades; no se afirma control por prefijo de ADLS ni máscaras UC efectivas hasta Fase 4.
- En esquemas vacíos, fallar al consultar una tabla inexistente **no prueba** denegación de `SELECT`: comprobar grants y propietarios efectivos y, para la prueba de lectura, usar únicamente un objeto sintético vacío de prueba y retirarlo, con cómputo temporal autorizado y detenido. Si no se autoriza ese coste, registrar la prueba de lectura como pendiente y no dar por cerrado el criterio de acceso efectivo de Fase 1.
- El acceso a `landing` queda separado: requiere identidad y grant de alcance mínimo propios. La máscara de columnas se mantiene para Fase 4; no hay PII real en Fase 1.
- Los documentos distinguen objetivo de coste/SLA de medición real y no afirman cumplimiento GDPR, recursos ni controles desplegados.

## Riesgos y rollback

La primera aplicación futura puede fallar por cuota, permisos de cuenta, metastore inexistente, disponibilidad de SKU u oferta no elegible. Una aplicación parcial puede dejar recursos facturables, roles o backend state que requieren revisión humana para retirarlos. El rollback se decide antes de ejecutar: inventario de recursos, owner, orden de retirada y conservación/rotación segura del state; no se improvisa borrando almacenamiento que contenga evidencia de datos. En particular, destruir `platform` abarca RG, ADLS, workspace, Access Connector y RBAC; no es una acción para detener compute. Los objetos opcionales del root `databricks` y el metastore/UC requieren revisión separada.

La aprobación de estos gates no sustituye verificaciones reales de RBAC/UC, conectividad, costes ni acceso: Fase 1 solo cierra tras apply autorizado que compruebe fundación Azure, metastore/asignación, DEFAULT managed location, propietarios y acceso efectivo de credential/location, `tt_ecommerce_synthetic` y sus cuatro esquemas. Se prueban identidades permitidas/no autorizadas y no se carga dato alguno; solo entonces Fase 2 puede cargar sintéticos. No incluye máscaras, PII real ni SQL Warehouse. La aprobación vigente es únicamente offline.
