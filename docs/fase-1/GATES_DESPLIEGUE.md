# Fase 1 — Gates de despliegue y validación offline

**Estado:** Fase 1 no está cerrada. Este documento define qué debe comprobarse antes de solicitar permiso de despliegue; durante su preparación solo se permite código, documentación y validación offline.

## Secuencia con gates

| Gate | Decisión/evidencia necesaria | Resultado si no pasa |
| --- | --- | --- |
| G0 — alcance | Confirmar entorno sintético, West Europe y prohibición de `apply`. | No se inicializa ningún recurso cloud. |
| G1 — coste/oferta | Confirmar oferta Free Trial, saldo/caducidad, elegibilidad de Databricks y cálculo retail actual de ADLS, workspace y futuros SKUs. | No se habilita despliegue; SQL/compute quedan fuera. |
| G2 — identidad | Tenant, suscripción, principal Terraform/CI, grupos Entra, owners y roles efectivos aprobados. | No se codifican IDs reales ni grants definitivos. |
| G3 — estado | Decidir y autorizar bootstrap de backend Blob protegido, o estado local temporal; acceso mínimo, recuperación y responsable documentados. El backend no se configura automáticamente. | No se usa backend remoto. |
| G4 — UC | Account admin confirma metastore, región compatible, asignación al workspace y propietario. | Workspace puede permanecer sin objetos UC; no se presupone storage credential/external location. |
| G5 — red | Aprobar expresamente el endpoint público de Storage/workspace alcanzable desde cualquier red por principals autenticados, sin reglas de red, **solo para sintético**; o elegir rediseño private/allowlist y documentar DNS, coste y acceso. | No se realiza aplicación ni se cargan datos reales. |
| G6 — revisión | Revisión humana de coste, RBAC, nombres, tags y rollback; permiso explícito para una futura aplicación. | Se conserva trabajo offline. |

## Checklist previo a una futura aplicación

- [ ] Oferta exacta, crédito, vencimiento y cobertura de Databricks confirmados por el usuario/portal; presupuesto y contactos de alertas configurados o aprobados.
- [ ] Calculadora retail de West Europe revisada con SKU y tiempo de ejecución esperado; no se deriva coste de las referencias históricas de esta documentación.
- [ ] Tenant/subscription, región cloud, cuota Databricks, roles y owners confirmados; ningún ID, token o secreto se añade a Git.
- [ ] Metastore UC/account admin y posibilidad de asignar workspace confirmados; la automatización de UC se separa si requiere paso manual.
- [ ] Patrón de red aprobado: se acepta expresamente que Storage y workspace públicos son alcanzables desde cualquier red por principals autenticados, sin allowlist, solo para sintético; o se aprueba un rediseño private/allowlist. TLS y ausencia de acceso anónimo no equivalen a red privada.
- [ ] Backend de estado aislado de ADLS de datos, con acceso mínimo y plan de recuperación; `tfstate` sigue fuera de Git.
- [ ] No se usarán identificadores/contactos reales en esta fase; retención, borrado y requisitos legales siguen pendientes.
- [ ] SQL Warehouse, clusters y jobs están deshabilitados por defecto; `PRO 2X-Small` no se habilita hasta revisar su precio exacto de DBU/VM, y cualquier excepción incluye auto-stop/autoterminación y revisión de coste.

## Validación permitida ahora

Sin credenciales ni llamadas cloud: formateo, inicialización Terraform con backend deshabilitado, validación estática y revisión de variables/documentación. No ejecutar `plan` contra la suscripción, `apply`, creación de metastore, asignaciones reales de RBAC ni carga de datos.

Criterios de revisión offline:

- La configuración describe solo el grafo MVP y los componentes opcionales están explícitamente desactivados o aislados.
- No hay secretos, `tfstate`, credenciales cloud ni valores reales sensibles en archivos versionados.
- ADLS declara HNS, transporte seguro y ausencia de acceso anónimo; se documenta que el endpoint público no tiene reglas de red y RBAC está limitado a principals parametrizados.
- BI/analistas no reciben RBAC Azure explícito a Bronze, Silver o cuarentena. No se afirma ACL por directorio ni grants UC efectivos: el conector se limita al contenedor `lakehouse` y aún no existe control por prefijo.
- El acceso futuro a `landing` queda separado: requiere identidad y grant de alcance mínimo propios; UC/máscaras se mantienen como prerrequisitos posteriores.
- Los documentos distinguen objetivo de coste/SLA de medición real y no afirman cumplimiento GDPR, recursos ni controles desplegados.

## Riesgos y rollback

La primera aplicación futura puede fallar por cuota, permisos de cuenta, metastore inexistente, disponibilidad de SKU u oferta no elegible. Una aplicación parcial puede dejar recursos facturables, roles o backend state que requieren revisión humana para retirarlos. El rollback se decide antes de ejecutar: inventario de recursos, owner, orden de retirada y conservación/rotación segura del state; no se improvisa borrando almacenamiento que contenga evidencia de datos.

La aprobación de estos gates no sustituye las verificaciones reales de RBAC/UC, conectividad, costes ni acceso por identidad: Fase 1 solo podrá cerrarse tras un despliegue live autorizado y dichas verificaciones. La aprobación vigente es únicamente para trabajo offline.
