# Fase 1 — Guía de despliegue guiado

**Estado:** preparación offline lista; Fase 1 sigue **abierta**. Esta guía no autoriza comandos, autenticación, `plan`, `apply`, `destroy` ni llamadas cloud. Cada ejecución la realiza el usuario y requiere una autorización explícita posterior a la revisión del paso anterior.

## Qué está listo y qué no acredita

El 2026-11-03 se validó offline solo `infra/databricks`: Terraform 1.13.5 temporal (SHA256 oficial), provider Databricks 1.80.0, `fmt -check -recursive`, `init -backend=false -lockfile=readonly` y `validate`, todos con código 0. No hubo credenciales, Azure, state ni plan; otros roots no están cubiertos. Esto acredita sintaxis/configuración local, no permisos, costes, propietarios ni acceso efectivo.

## Etapa 0 — Hechos de portal y decisiones humanas

Antes de ejecutar comandos Terraform que consulten Azure, el usuario comprueba en el portal y confirma sin compartir IDs, tokens, state ni secretos:

- oferta de suscripción, saldo/caducidad y elegibilidad Databricks;
- cuota disponible en France Central; coste persistente estimado de workspace/ADLS, presupuesto, alertas y owner;
- tenant, roles del principal aplicador, metastore UC existente compatible con France Central y asignable, DEFAULT managed storage, nombre visible y owner del grupo de cuenta escritor existente;
- elección de backend/state, acceso mínimo y recuperación;
- aprobación explícita de endpoint público, solo para sintéticos.

La región prevista para `platform` y `bootstrap` opcional es France Central. West Europe mostró al usuario una advertencia de no admisión de nuevos clientes; que no aparezca en France Central no garantiza cuota, Free Trial ni Premium. No hace falta precio exacto de SQL PRO hasta Fase 5. Si falta cualquier punto, el resultado es **PAUSA**. Los tres booleanos UC son afirmaciones humanas, no tests; sin DEFAULT no se usa `storage_root` alternativo ni la external location como managed storage: se pausa y rediseña con UC admin.

## Etapa 1 — Preparación local del usuario

**Usuario:** ejecuta cualquier comando Terraform solo si lo decide y tras la revisión acordada; la validación previa solo cubre `databricks`, no la configuración actual de `platform`/`bootstrap`. Prepara localmente, sin versionar ni pegar en el chat, variables, secretos y cualquier archivo de backend. No usar Portal **Create** como sustituto de los roots Terraform.

**Pausa/revisión:** compartir únicamente un resumen redactado de resultados y cambios previstos. Si aparecen secretos, IDs, coste inesperado, endpoint no aprobado o discrepancia de provider, parar.

## Etapa 2 — Inspección y plan por root

Con permiso explícito independiente para inspección cloud, el usuario sigue el orden `bootstrap` opcional en France Central → `platform` en France Central → `databricks`. Antes del root Databricks inventaría grants y owners existentes del metastore, storage credential y external location, y confirma identidad aplicadora/owner dedicada y membresía exacta del grupo.

**Usuario:** ejecuta el plan de un solo root y comparte un resumen redactado: recursos a crear/cambiar, coste/riesgo, owners/grants esperados y bloqueos. No compartir state ni credenciales.

**Asistente:** revisa el resumen. **No hay apply implícito:** el usuario debe autorizar por separado cada apply. Si el plan amplía privilegios, crea SQL, no respeta red/coste o no permite identificar owner, el resultado es **PAUSA**.

## Etapa 3 — Apply y evidencia por root

Tras autorización separada, el usuario aplica un root y conserva evidencia redactada de coste, RBAC y objetos creados. El metastore/account-admin assignment puede requerir operación manual externa.

Para `databricks`, comprobar antes de datos: credencial/external location, `tt_ecommerce_synthetic`, esquemas vacíos `bronze`, `silver`, `quarantine`, `gold`; propietario de cada objeto; y grants efectivos. El IaC declara únicamente `USE_CATALOG` y `USE_SCHEMA`/`CREATE_TABLE` para el grupo escritor. No demuestra que sea el único acceso: herencias, grants preexistentes y owners pueden otorgar más.

Probar con identidades reales permitidas y no autorizadas. BI, stewardship y analistas deben fallar lecturas y escrituras. Una tabla inexistente no prueba denegación: para una prueba de lectura se necesita un objeto temporal **vacío y sintético**, compute transitorio autorizado, evidencia del resultado y retirada posterior. Si no se autoriza ese compute, registrar el test como pendiente y mantener Fase 1 abierta. El Access Connector/external location cubre el contenedor `lakehouse` completo, no zonas/prefijos.

## Etapa 4 — Cierre o recuperación

**Pasa Fase 1** solo cuando la evidencia autorizada confirma fundación, costes aceptados, UC/owners/acceso efectivo y pruebas negativas, sin datos cargados. Entonces Fase 2 puede cargar sintéticos. Máscaras quedan en Fase 4 y SQL Warehouse en Fase 5.

Ante apply parcial, bloqueo o state inconsistente: pausar, preservar state/evidencia, inventariar recursos y pedir revisión. No usar `-target` ni `destroy` como limpieza. Conservar workspace y ADLS; detener compute cuando exista según el [ciclo de desarrollo](CICLO_DE_DESARROLLO.md).

## Bloqueadores que debe aportar o confirmar el usuario

1. Hechos de Etapa 0, especialmente coste/cuota/red y owner de alertas.
2. Identidad aplicadora/owner aprobada y capacidad de UC account/metastore admin.
3. DEFAULT managed storage, grupo escritor existente y membresía estrecha verificables.
4. Elección/recuperación del backend y autorización explícita, paso a paso, para inspección, cada plan y cada apply.
