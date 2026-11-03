# Fase 1 — Gates de despliegue y validación offline

**Estado:** Fase 1 no está cerrada. Este documento define qué debe comprobarse antes de solicitar permiso de despliegue; durante su preparación solo se permite código, documentación y validación offline. La secuencia operativa humana está en la [guía de despliegue guiado](GUIA_DESPLIEGUE_GUIADO.md); no autoriza operaciones por sí misma.

## Secuencia con gates

| Gate | Decisión/evidencia necesaria | Resultado si no pasa |
| --- | --- | --- |
| G0 — alcance | Confirmar entorno sintético, France Central y prohibición de `apply`. La advertencia portal de West Europe no prueba disponibilidad France Central. | No se inicializa ningún recurso cloud. |
| G1 — coste/oferta | Confirmar oferta/crédito/saldo/caducidad, elegibilidad Databricks Premium, cuota France Central y coste actual de ADLS/workspace, presupuesto y alertas. | No se habilita despliegue; SQL/compute quedan fuera. |
| G2 — identidad | Tenant, suscripción, principal Terraform/CI, grupos Entra, owners y roles efectivos aprobados. | No se codifican IDs reales ni grants definitivos. |
| G3 — estado | Backend decidido: Storage separado, contenedor privado, OAuth/Entra-RBAC, Shared Key/usuarios locales deshabilitados, endpoint público sin allowlist como excepción registrada y protección de 7 días. El usuario reporta RG, Storage Standard LRS, contenedor `tfstate` y role assignment Blob Data Contributor de contenedor aplicados; bootstrap conserva estado local. El init previo de platform quedó local porque faltaba bloque backend, pero el usuario reporta que el backend remoto quedó configurado antes de Stage 1. | Conservar evidencia redactada; no se infiere acceso a datos ni otros controles del backend. |
| G4 — UC | Las tres variables default-false se habilitan solo tras confirmar metastore asignado/identidad aplicadora, DEFAULT managed location y un único grupo escritor existente aprobado; verificar soporte/permisos del provider 1.80.0. | No se aplica el root Databricks ni se cargan datos. Sin DEFAULT, bloquear y rediseñar con UC admin. |
| G5 — red | **APROBADO/registrado:** endpoints públicos de Storage/workspace alcanzables desde cualquier red solo por principals autenticados, sin allowlist, para datos sintéticos exclusivamente. Private Link/VNet queda diferido a diseño separado. TLS, blob anónimo deshabilitado y RBAC no equivalen a red privada. | No autoriza apply ni acredita endpoints/control live; nunca PII ni datos reales. |
| G6 — revisión | Revisión humana de coste, RBAC, nombres, tags y rollback; permiso explícito para una futura aplicación. | Se conserva trabajo offline. |

## Checklist previo a una futura aplicación

- [ ] Oferta exacta, crédito, vencimiento y cobertura de Databricks confirmados por el usuario/portal; presupuesto y contactos de alertas configurados o aprobados.
- [ ] Coste persistente y cuota de France Central revisados para ADLS/workspace; presupuesto, alertas y owner aprobados. La señal observada en West Europe no garantiza France Central; no reutilizar cifras West Europe. SQL PRO se revisa solo en Fase 5.
- [ ] El usuario reporta un presupuesto mensual de suscripción de 50 EUR con alertas al 40% y 100%; confirmar alcance, moneda y destinatarios/notificaciones antes de apply. No es un tope técnico de gasto.
- [ ] Tenant/subscription, región cloud, cuota Databricks, roles y owners confirmados; ningún ID, token o secreto se añade a Git.
- [ ] Metastore UC compatible con France Central, asignación al workspace e identidad aplicadora con autoridad de objeto confirmados (`unity_catalog_prerequisites_confirmed`); Terraform no crea/asigna el metastore.
- [ ] Ubicación administrada DEFAULT aprobada para el metastore (`metastore_default_managed_storage_confirmed`). El catálogo omite `storage_root`; no usar la external location de `lakehouse` como managed storage. Si falta DEFAULT, bloquear y rediseñar con UC admin.
- [ ] Nombre no versionado de un único grupo escritor existente aprobado (`approved_engineer_writer_group_confirmed`); no usar `account users` ni comodines. El grant se limita a `USE_CATALOG` y, por esquema, `USE_SCHEMA`/`CREATE_TABLE`.
- [ ] Tratar los tres booleanos como afirmaciones humanas, no como tests: inventariar grants/propietarios preexistentes del metastore, credential y external location; aprobar identidad aplicadora/propietaria dedicada y membresía exacta del grupo antes del apply.
- [ ] Soporte y permisos efectivos del provider Databricks 1.80.0 para credencial, external location, catálogo, esquemas y grants verificados; no hay evidencia de ello hasta un despliegue autorizado.
- [x] G5 registrado: Storage y workspace públicos, sin allowlist, accesibles desde cualquier red solo para principals autenticados y solo con datos sintéticos. Private Link/VNet se difiere. TLS, blobs anónimos deshabilitados y RBAC no equivalen a red privada ni a cumplimiento GDPR; el despliegue/control efectivo sigue sin verificar.
- [ ] Antes del plan bootstrap autorizado: consultar sin modificar el registro de `Microsoft.Storage`, `Microsoft.Authorization` y `Microsoft.Databricks`. El usuario reporta este último como Registered; no constituye evidencia live. Con `resource_provider_registrations = "none"` previsto en platform/bootstrap, no habrá autorregistro y cualquier proveedor requerido no registrado bloquea el proceso.
- [x] El usuario reporta backend remoto de `platform` configurado y Stage 1 autorizado aplicado: nueve recursos esperados, sin rutas Gen2. No se dispone de evidencia cloud directa en el repositorio.
- [x] El usuario reporta probes ADLS Gen2 Entra autenticados y de solo lectura sobre `landing` y `lakehouse`; ambos devolvieron OK. Esto acredita solo según reporte acceso de plano de datos del operador CLI actual a cada contenedor, no acceso de CI ni UC. No se registran contenido ni IDs.
- [x] El usuario reporta Stage 2 revisado/autorizado aplicado: exactamente siete rutas Gen2 creadas y ningún cambio ni destrucción. Los flags `create_data_lake_directories` y `terraform_operator_blob_access_confirmed` permanecen `true` y nunca deben volver a `false`, pues destruirían las rutas. No usar `-target`; futuros plan/apply siguen requiriendo revisión y autorización explícitas.
- [ ] Confirmar recuperación de state: versionado de blob y soft delete de blob/contenedor de 7 días. Endpoint público sin allowlist solo sirve a principals autenticados; no vuelve público el `tfstate` sensible ni equivale a red privada/GDPR.
- [ ] No se usarán identificadores/contactos reales en esta fase; retención, borrado y requisitos legales siguen pendientes.
- [ ] SQL Warehouse, clusters y jobs están deshabilitados por defecto. El Warehouse sigue como código opt-in `false` y no se crea en Fase 1; se reconsidera en Fase 5, con Gold existente, precio/cuota/permisos confirmados y auto-stop antes de Power BI.
- [ ] Si se autoriza compute, owner y runbook de [ciclo de desarrollo](CICLO_DE_DESARROLLO.md) revisados: Jobs compute por ejecución, autoterminación para all-purpose, comprobación de estados y parada manual de SQL tras demos.
- [ ] Se acepta que `platform` se conserva entre sesiones y que ADLS/recursos persistentes pueden tener coste; presupuesto/alertas no son límite duro. No se empleará `terraform destroy` ni `-target` para parar compute.

## Validación permitida ahora

El 2026-11-03 el usuario ejecutó validación offline con Terraform temporal 1.13.5 Windows amd64 verificado. `platform`: init con azurerm 4.30.0, corrección previa del `resource_manager_id` deprecado por el ID del contenedor sin ampliar alcance, después fmt-check/validate con código 0 y sin avisos. `bootstrap`: fmt-check/init azurerm 4.30.0/validate, código 0. `databricks`: fmt-check/init provider 1.80.0/validate, código 0. Los init usaron `-backend=false -lockfile=readonly`; `.terraform` quedó ignorado y lockfiles sin cambios. El usuario reporta bootstrap, backend remoto funcional, Stage 1, probes ADLS Gen2 correctos y Stage 2 aplicado con exactamente siete rutas, sin cambios ni destrucciones. Esta documentación no aporta evidencia cloud directa; el root `databricks` no está aplicado.

Criterios de revisión offline:

- La configuración describe solo el grafo MVP y los componentes opcionales están explícitamente desactivados o aislados.
- No hay secretos, `tfstate`, credenciales cloud ni valores reales sensibles en archivos versionados.
- ADLS declara HNS, transporte seguro y ausencia de acceso anónimo; se documenta que el endpoint público no tiene reglas de red y RBAC está limitado a principals parametrizados.
- BI, analistas y stewardship no reciben RBAC Azure explícito ni grants UC declarados por este IaC en Fase 1. Eso no revoca grants heredados/preexistentes ni poderes de propietario/creador. Antes de cargar datos, tras apply autorizado se verifican propietario y acceso efectivo del catálogo, cuatro esquemas, credential y external location, con negativas de lectura/escritura para esas identidades; no se afirma control por prefijo de ADLS ni máscaras UC efectivas hasta Fase 4.
- En esquemas vacíos, fallar al consultar una tabla inexistente **no prueba** denegación de `SELECT`: comprobar grants y propietarios efectivos y, para la prueba de lectura, usar únicamente un objeto sintético vacío de prueba y retirarlo, con cómputo temporal autorizado y detenido. Si no se autoriza ese coste, registrar la prueba de lectura como pendiente y no dar por cerrado el criterio de acceso efectivo de Fase 1.
- El acceso a `landing` queda separado: requiere identidad y grant de alcance mínimo propios. La máscara de columnas se mantiene para Fase 4; no hay PII real en Fase 1.
- Los documentos distinguen reporte de usuario de evidencia cloud directa, objetivo de coste/SLA y medición real; no afirman cumplimiento GDPR ni controles verificados.

## Riesgos y rollback

La primera aplicación futura puede fallar por cuota, permisos de cuenta, metastore inexistente, disponibilidad de SKU u oferta no elegible. Una aplicación parcial puede dejar recursos facturables, roles o backend state que requieren revisión humana para retirarlos. El rollback se decide antes de ejecutar: inventario de recursos, owner, orden de retirada y conservación/rotación segura del state; no se improvisa borrando almacenamiento que contenga evidencia de datos. En particular, destruir `platform` abarca RG, ADLS, workspace, Access Connector y RBAC; no es una acción para detener compute. Los objetos opcionales del root `databricks` y el metastore/UC requieren revisión separada.

La aprobación de estos gates no sustituye verificaciones reales de RBAC/UC, conectividad, costes ni acceso: Fase 1 solo cierra tras apply autorizado que compruebe fundación Azure, metastore/asignación, DEFAULT managed location, propietarios y acceso efectivo de credential/location, `tt_ecommerce_synthetic` y sus cuatro esquemas. Se prueban identidades permitidas/no autorizadas y no se carga dato alguno; solo entonces Fase 2 puede cargar sintéticos. No incluye máscaras, PII real ni SQL Warehouse. La aprobación vigente es únicamente offline.
