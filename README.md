# TT E-commerce Lakehouse

Proyecto de portfolio para diseñar un e-commerce ficticio de material de tenis de mesa y su plataforma analítica gobernada en Azure. El objetivo no es operar una tienda real: es demostrar cómo transformar eventos de venta sintéticos en métricas fiables para negocio, manteniendo el acceso a datos personales bajo control.

> **Estado actual:** Fase 0 cerrada; Fase 1 en preparación offline con Terraform y controles previos al despliegue. No se han desplegado recursos de Azure ni creado notebooks o dashboards todavía.

## Problema y narrativa comercial

La tienda ficticia vende palas, gomas, pelotas, mesas, redes y accesorios. Operaciones y comercial quieren responder preguntas como:

- ¿Qué categorías, regiones y canales generan más ingresos y pedidos?
- ¿Qué productos tienen mejor rotación y qué pedidos se han reembolsado?
- ¿Cómo evoluciona la facturación diaria sin exponer datos de contacto de clientes a quienes solo consumen BI?

La fuente operativa se simulará con archivos o eventos de pedidos, clientes y catálogo. La plataforma conservará el origen para trazabilidad, depurará y conformará los datos antes de publicar agregados aptos para análisis.

## Objetivos y alcance

### MVP

- Datos **sintéticos** reproducibles: pedidos JSONL y archivos CSV de clientes y catálogo, con casos de duplicados, anulaciones, nulos y cambios de esquema controlados.
- Lago en ADLS Gen2 y carga incremental a Delta Bronze mediante Databricks Auto Loader, con checkpoints y captura de registros no interpretados (`rescued data`).
- Transformaciones PySpark a Silver: contrato de esquema, tipado, deduplicación idempotente, reglas de calidad, cuarentena y columnas de auditoría.
- Unity Catalog para gobernanza: en Fase 1, con gates explícitos, el código prepara el catálogo sintético `tt_ecommerce_synthetic`, los esquemas vacíos `bronze`, `silver`, `quarantine` y `gold`, y grants mínimos para un único grupo escritor existente. No hay despliegue ni evidencia live; las políticas de enmascaramiento dinámico se implementarán y probarán en la Fase 4.
- Tablas Gold agregadas sin columnas PII previstas para consumo general. El SQL Warehouse se creará en la Fase 5, únicamente tras su gate de coste, antes del informe Power BI inicial.
- Terraform como infraestructura declarativa, con estado remoto protegido cuando se disponga del backend; el estado y las credenciales nunca se versionarán.

### Ampliaciones (fuera del MVP)

- CDC o streaming de pedidos, SCD tipo 2 para clientes/productos y orquestación programada.
- Alertas operativas, catálogo/lineage más completo, pruebas de rendimiento y despliegue CI/CD.
- Análisis avanzado de riesgo de reidentificación y controles adicionales para combinaciones de dimensiones; los criterios básicos de publicación ya forman parte del MVP.
- Evaluación de DirectQuery frente a Import en Power BI según latencia, modelo semántico y coste del SQL Warehouse.

## Arquitectura propuesta

```mermaid
flowchart LR
    S[Datos sintéticos\norders · customers · products] --> A[ADLS Gen2\nlanding]
    A --> B[Databricks Auto Loader\ncheckpoint + rescued data]
    B --> C[Delta Bronze\nraw, append-only]
    C --> D[PySpark + reglas de calidad]
    D --> E[Delta Silver\nlimpio, deduplicado, restringido]
    D --> Q[Cuarentena\nrechazos explicables]
    E --> G[Delta Gold\nagregados de negocio]
    G --> W[Databricks SQL Warehouse]
    W --> P[Power BI\nImport inicial; DirectQuery a validar]
    UC[Unity Catalog\nidentidad, permisos, políticas] --- C
    UC --- E
    UC --- G
```

Flujo esperado:

1. Un generador determinista produce datos sintéticos y un manifiesto de ejecución.
2. Los archivos llegan al área `landing` de ADLS. Auto Loader descubre únicamente archivos nuevos y persiste el progreso en su checkpoint.
3. Bronze almacena el payload recibido, metadatos de ingestión, archivo origen y sistema fuente de forma append-only.
4. Silver aplica el contrato, normaliza tipos y códigos, conserva columnas de auditoría y hace `MERGE` idempotente sobre una clave de negocio y una marca de evento/versión. Los registros inválidos se desvían a cuarentena, no se corrigen silenciosamente.
5. Gold recalcula ventanas explícitas sobre una instantánea completa de Silver y publica métricas de pedidos, ingresos, categoría, región y estado. Un refresco debe poder eliminar resultados obsoletos de una fecha sin alterar fechas fuera de su ventana.

## Modelo de datos inicial

| Entidad | Ejemplos de campos | Uso |
| --- | --- | --- |
| `orders` | `order_id`, `line_id`, `customer_id`, `product_id`, `order_date`, `status`, `quantity`, `revenue`, moneda | Hechos de venta, devolución o cancelación. Grano inicial: línea de pedido (`order_id` + `line_id`). |
| `customers` | `customer_id`, país/región, email, teléfono, fechas de auditoría | Relación con cliente; datos de contacto sintéticos y restringidos. |
| `products` | `product_id`, nombre, categoría, marca, precio | Dimensión de catálogo: palas, gomas, pelotas, mesas y accesorios. |

La deduplicación de pedidos se definirá sobre `order_id` + `line_id` y una versión/marca temporal de evento; una corrección o devolución debe sustituir el estado vigente de esa línea sin borrar la evidencia Bronze. Las métricas de ingresos usarán el neto de las versiones vigentes y estados definidos en el contrato, mientras que los pedidos se contarán como `count(distinct order_id)` para no inflarlos por número de líneas. Las claves, formatos y semántica están documentados en la Fase 0; el volumen de infraestructura se decidirá antes de desplegar. Los datos sintéticos no deben copiar identidades ni contactos reales.

## Capas, consumidores y acceso

| Capa | Propósito | Consumidores permitidos previstos |
| --- | --- | --- |
| Bronze | Evidencia raw e inmutable para replay y diagnóstico. | Ingeniería de datos con necesidad operativa. |
| Silver | Datos limpios y conformados; puede contener identificadores y contacto controlado. | Ingeniería y stewardship con necesidad justificada; analistas autorizados, únicamente mediante una vista/subconjunto aprobado con contacto enmascarado. No se abrirá de forma indiscriminada. |
| Cuarentena | Filas rechazadas y motivo de rechazo. | Ingeniería de datos / soporte autorizado. |
| Gold | Métricas y dimensiones de negocio publicadas. | Analítica, SQL Warehouse y Power BI según permisos. |

Los roles/grupos concretos se crearán o mapearán a identidades reales disponibles en el tenant; estos nombres son provisionales y no sustituyen esa verificación:

| Grupo | Acceso previsto |
| --- | --- |
| `data-engineers` | Bronze/Silver/cuarentena necesarios para operar pipelines; email/teléfono originales solo si su función lo requiere. |
| `customer-data-stewards` | Subconjunto Silver justificado para gestión de datos de cliente; contacto original. |
| `silver-analysts-authorized` | Solo una vista o subconjunto Silver aprobado; email/teléfono enmascarados dinámicamente. Sin `SELECT` general sobre tablas Silver. |
| `bi-executives` | Solo vistas/tablas Gold publicadas a través del SQL Warehouse/Power BI; sin Bronze, Silver ni cuarentena. |

Antes de cargar datos en Bronze/Silver, Unity Catalog deberá tener el metastore asignado y una ubicación administrada DEFAULT aprobada; los catálogos/esquemas base y los grants mínimos deberán verificarse con identidades reales. El catálogo no define `storage_root`: si no hay DEFAULT aprobada, se bloquea y se rediseña con el administrador UC. El IaC de Fase 1 solo declara para un grupo escritor `USE_CATALOG` y, por esquema, `USE_SCHEMA`/`CREATE_TABLE`; no declara grants para analistas, BI, stewardship o `account users`. Esto no prueba que sean los únicos accesos efectivos: grants heredados/preexistentes y propietarios pueden prevalecer. El enmascaramiento dinámico es trabajo de la Fase 4; no se declara configurado ni se cargarán PII reales.

Gold evitará publicar PII por diseño. Eso no garantiza que un agregado de baja cardinalidad no permita inferencias; antes de exponerlo se definirán criterios de tamaño mínimo, supresión o generalización acordes al caso de uso.

## Seguridad y gestión de datos

- Principio de mínimo privilegio entre identidades administradas, ADLS, Databricks, Unity Catalog y consumidores BI.
- Secretos en Key Vault o en el mecanismo corporativo aprobado, referenciados en tiempo de ejecución. No habrá secretos, tokens, ficheros de estado Terraform ni valores de configuración sensibles en Git.
- Si se necesitan identificadores derivados, se evaluará un hash con salt gestionado como secreto y rotación. Un hash no equivale automáticamente a anonimización ni elimina la necesidad de controles de acceso.
- Minimización, retención, borrado y auditoría son objetivos de diseño pendientes de concretar con requisitos de negocio y plataforma. Este proyecto no declara certificación ni cumplimiento GDPR.

## Stack previsto

- Azure Data Lake Storage Gen2 para aterrizaje y almacenamiento Delta.
- Azure Databricks: Auto Loader, PySpark, Delta Lake, Workflows y SQL Warehouse.
- Unity Catalog para metadatos, permisos, auditoría disponible y políticas de columnas.
- Power BI para el consumo de Gold.
- Terraform para infraestructura y configuración que el proveedor soporte en el entorno elegido; se validará la cobertura y los prerrequisitos al implantar.
- Python/PySpark, SQL, pruebas de calidad y CI para el código de datos.

## Decisiones, prerrequisitos y coste

- La región prevista para `platform` y para el backend opcional `bootstrap` es **France Central**. El asistente de Azure mostró una advertencia de no admisión de nuevos clientes en West Europe y no la mostró en France Central; es una señal observada por el usuario, no una garantía de cuota, Free Trial ni cobertura Premium. El usuario reporta que el backend remoto de `platform` funciona y que Stage 1 y Stage 2 se aplicaron; no hay evidencia cloud directa en este repositorio. La configuración del root `databricks`/UC aún no está aplicada.
- La decisión G5 registrada permite endpoints públicos de Storage y Databricks desde cualquier red para principals autenticados, sin allowlist, exclusivamente con datos sintéticos. TLS, RBAC y blobs anónimos deshabilitados no equivalen a red privada ni a cumplimiento GDPR; PII/datos reales no se cargarán y Private Link/VNet queda diferido.
- Unity Catalog requiere que el workspace, metastore, almacenamiento administrado/external locations, identidades y permisos se preparen de forma compatible con la configuración Azure/Databricks elegida. Decisión aprobada: no se fija ni modifica el root DEFAULT del metastore existente; permanece vacío. `tt_ecommerce_synthetic` usará managed storage a nivel de catálogo en un child path exclusivo bajo el almacenamiento dedicado separado, mientras `lakehouse` queda aislado para su external location. Esto evita el riesgo ForceNew del provider 1.80 sobre el root de un metastore existente y ambigüedad entre root/external location. Stage 3 (storage/conector/RBAC dedicados) está reportado como aplicado. El usuario reporta que la identidad cloud-only es Account Admin, Workspace Admin y único Metastore Admin, mediante transferencia autorizada por el límite de un admin de metastore. No tiene roles de suscripción Azure reportados. Su Global Administrator Entra temporal se conserva hasta verificar acceso cloud/provider y criterios de recuperación. No hay catálogo, external location, credencial ni otros objetos UC; ningún plan futuro debe mutar o reemplazar el metastore.
- El backend de estado usa un Storage Account independiente con endpoint público sin allowlist solo para principals Entra/RBAC autenticados, contenedor privado, Shared Key y usuarios locales deshabilitados, OAuth por defecto y versionado/soft delete de blob y contenedor de 7 días. Es una excepción de red separada de G5: `tfstate` es sensible (IDs y valores de recurso potencialmente sensibles), no contiene PII por diseño y no se vuelve público por tener endpoint público; no equivale a red privada ni a cumplimiento GDPR. El usuario reporta que bootstrap creó RG de estado, Storage Account Standard LRS y contenedor privado `tfstate` en France Central, y después el role assignment de contenedor para la identidad CLI actual. El estado propio de bootstrap sigue local. El init anterior de platform quedó inicialmente local al faltar bloque backend; el usuario reporta que el backend remoto de platform ya se configuró antes del apply Stage 1.
- La topología final, SKU, horarios de apagado, retención y tamaño de cómputo se decidirán tras una estimación de volumen y presupuesto. El SQL Warehouse permanece como código opt-in desactivado y no se creará en Fase 1; su creación en Fase 5 exige una revisión de coste antes de Power BI.
- Algunas operaciones de Databricks/Azure pueden requerir pasos administrativos o no estar disponibles en la versión del provider elegida; se comprobarán en el momento de despliegue en vez de asumir soporte total.
- Para AzureRM 4.30.0, los roots `platform`/`bootstrap` usarán `resource_provider_registrations = "none"`: no autorregistrarán providers. Un plan posterior requerirá CLI login Owner y `ARM_SUBSCRIPTION_ID` solo local; nunca se versiona ni comparte. Antes del plan bootstrap autorizado se inspeccionan en lectura los registros `Microsoft.Storage`, `Microsoft.Authorization` y `Microsoft.Databricks`.
- El usuario reporta Stage 1 de `platform` (fundación, Storage HNS, contenedores privados, workspace, Access Connector y RBAC acotado) y los probes Entra de lectura correctos. También reporta que el Stage 2, revisado y autorizado por separado, creó exactamente siete rutas Gen2 sin cambios ni destrucciones: `landing/orders`, `landing/customers`, `landing/products`, `lakehouse/bronze`, `lakehouse/silver`, `lakehouse/quarantine` y `lakehouse/gold`. Los dos flags locales de Stage 2 permanecen en `true` y nunca deben resetearse a `false`, pues destruirían las rutas; no usar `-target`. Esto no implica acceso CI/UC ni autorización de futuros applies.

## Probar la fase 0 en local

Requiere Python 3.13; el generador y sus pruebas solo usan la biblioteca estándar. En Windows, desde la raíz del repositorio:

```powershell
py scripts/generate_synthetic_data.py --seed 42 --batch-id DEMO-001
py -m unittest discover -s tests -v
```

En macOS/Linux se puede sustituir `py` por `python3`. El lote se guarda en `data/generated/DEMO-001/` (ignorado por Git); `data/sample/DEMO-001/` contiene un ejemplo versionado reproducible con la misma semilla. **Contiene anomalías deliberadas** —incluida una línea JSON malformada— para probar reglas de calidad; no debe tratarse como fuente completamente válida ni contiene contactos reales. Los importes, métricas y reglas esperadas se especifican en [fase 0](docs/fase-0/CONTRATOS.md).

## Ejecución prevista en Azure

Cuando se implemente la infraestructura, el repositorio documentará sus comandos y variables exactos. A alto nivel, la ejecución será:

1. Confirmar France Central, suscripción/tenant, identidades, backend de Terraform y permisos requeridos; no crear recursos manualmente desde el portal. Bootstrap remoto y backend de platform están reportados como configurados; los probes de lectura del operador en ambos contenedores también están reportados como correctos.
2. Tras autorización, aplicar la fundación y el root Databricks: verificar metastore/asignación e identidad aplicadora, ubicación administrada DEFAULT, credencial/ubicación externa, catálogo/esquemas vacíos y el grant mínimo del único grupo escritor.
3. Generar datos sintéticos, subirlos al área de aterrizaje y ejecutar la ingesta Bronze.
4. Ejecutar Silver, revisar cuarentena y métricas de calidad; solo entonces refrescar Gold.
5. Completar en Fase 4 las pruebas de permisos y máscaras por identidad; en Fase 5, crear el SQL Warehouse solo tras el gate de coste y conectar Power BI.

El 2026-11-03 el usuario validó offline los tres roots con Terraform temporal 1.13.5 Windows amd64 (SHA256 oficial): `platform` (azurerm 4.30.0; tras corregir el alcance deprecado, `fmt -check -recursive` y `validate` sin avisos), `bootstrap` (azurerm 4.30.0; fmt/init/validate correctos) y `databricks` (provider 1.80.0; fmt/init/validate correctos). Los `init` usaron backend deshabilitado y lockfile readonly. El usuario reporta bootstrap, backend remoto funcional, Stage 1, probes ADLS Gen2 correctos y Stage 2 aplicado con siete rutas sin cambios ni destrucciones. El root `databricks`/UC sigue sin apply. Esto no acredita cuota, coste, uso real, acceso futuro de CI ni controles UC.

## Roadmap

El plan por fases, dependencias, entregables y criterios verificables está en [docs/PLAN_DE_DESARROLLO.md](docs/PLAN_DE_DESARROLLO.md). La Fase 0 cerrada cubre [contratos](docs/fase-0/CONTRATOS.md#acta-de-decisiones-f0-v1), [métricas y calidad](docs/fase-0/METRICAS_Y_CALIDAD.md) y [linaje y privacidad](docs/fase-0/LINAJE_Y_PRIVACIDAD.md). La Fase 1 sigue en curso: [arquitectura y decisiones](docs/fase-1/ARQUITECTURA_Y_DECISIONES.md), [gates](docs/fase-1/GATES_DESPLIEGUE.md), [guía de despliegue guiado](docs/fase-1/GUIA_DESPLIEGUE_GUIADO.md) y [ciclo de desarrollo](docs/fase-1/CICLO_DE_DESARROLLO.md). Las Fases 2–3 construirán Bronze/Silver, la Fase 4 aplicará máscaras y pruebas de acceso, y la Fase 5 habilitará SQL Warehouse/Power BI tras el gate de coste. Ninguna evidencia local acredita despliegue Azure, UC activo, Gold, SQL Warehouse ni cumplimiento normativo.

Para controlar el gasto en futuras sesiones, se conservarán workspace y ADLS y se detendrá el cómputo; el procedimiento previsto está en [ciclo de desarrollo](docs/fase-1/CICLO_DE_DESARROLLO.md). No hay datos, jobs, SQL ni UC aplicados; el workspace/ADLS Stage 1 se describen solo según el reporte del usuario.
