# Trabajo Final — BigQuery for Analytics

Proyecto final del modulo **BigQuery for Analytics** del Diplomado en Data Analytics.

El objetivo es construir un **Data Warehouse** completo en Google BigQuery, implementando buenas practicas de gobernanza de datos: separacion en capas, control de accesos por roles y seguridad a nivel de filas (Row-Level Security).

---

## Tabla de contenidos

- [Contexto del proyecto](#contexto-del-proyecto)
- [Arquitectura del Data Warehouse](#arquitectura-del-data-warehouse)
- [Estructura de archivos](#estructura-de-archivos)
- [Parte 1 — Preparacion del Data Warehouse](#parte-1--preparacion-del-data-warehouse)
- [Parte 2 — Control de Accesos](#parte-2--control-de-accesos)
- [Parte 3 — Seguridad a Nivel de Filas](#parte-3--seguridad-a-nivel-de-filas)
- [Requisitos](#requisitos)
- [Guia de ejecucion](#guia-de-ejecucion)
- [Tecnologias utilizadas](#tecnologias-utilizadas)

---

## Contexto del proyecto

Se trabaja con un dataset de **30 registros de ventas del 2024** de una empresa con operaciones en tres regiones de Peru (Lima, Arequipa y Trujillo). Los datos incluyen informacion de productos, cantidades, precios, descuentos y vendedores.

El proyecto simula un escenario real donde:
- Un **Data Engineer** gestiona la capa de staging (ingesta de datos crudos)
- Un **Analista de Ventas** solo puede consultar datos agregados de su region asignada
- Las politicas de acceso se implementan tanto a nivel de dataset como a nivel de fila

---

## Arquitectura del Data Warehouse

```
                        GOOGLE BIGQUERY
 ┌──────────────────────────────────────────────────────────┐
 │                                                          │
 │   ventas_2024.csv                                        │
 │         │                                                │
 │         ▼                                                │
 │   ┌──────────────┐    ┌──────────────┐    ┌────────────────────┐
 │   │  dw_staging   │    │   dw_core     │    │    dw_serving       │
 │   │              │    │              │    │                    │
 │   │  stg_ventas  │───▶│ fact_ventas  │───▶│ v_resumen_ventas   │
 │   │  (raw data)  │    │ (+ total     │    │ (region, categoria,│
 │   │              │    │  calculado)  │    │  vendedor, metricas│
 │   └──────────────┘    └──────────────┘    └────────────────────┘
 │                              │                                │
 │                        ┌─────┴─────┐                          │
 │                        │    RLS     │                          │
 │                        ├───────────┤                          │
 │                        │ rls_lima  │ → solo region Lima        │
 │                        │ rls_admin │ → todas las regiones      │
 │                        └───────────┘                          │
 └──────────────────────────────────────────────────────────┘

 PERMISOS POR DATASET:
 ┌────────────────────┬─────────────┬──────────┬──────────────┐
 │ Service Account    │ dw_staging  │ dw_core  │ dw_serving   │
 ├────────────────────┼─────────────┼──────────┼──────────────┤
 │ analista-ventas-sa │ Sin acceso  │ Viewer*  │ Data Viewer  │
 │ data-engineer-sa   │ Data Editor │ Viewer*  │ Sin acceso   │
 └────────────────────┴─────────────┴──────────┴──────────────┘
 * Viewer en dw_core es necesario para que las politicas RLS funcionen
```

---

## Estructura de archivos

```
curse_claudia_proyecto_final/
├── parte1_data_warehouse.py     # Crea datasets, carga CSV, fact table y vista
├── parte2_control_accesos.py    # Crea service accounts y asigna permisos
├── parte2_validacion.py         # Valida permisos (EXITO / ACCESO DENEGADO)
├── parte3_rls.py                # Crea politicas RLS y verifica filtros
├── queries.sql                  # Todos los queries SQL del proyecto
├── ventas_2024.csv              # Datos fuente (30 registros de ventas)
├── .gitignore                   # Excluye llaves JSON, venv, cache
└── README.md
```

---

## Parte 1 — Preparacion del Data Warehouse

### 1.1 Creacion de datasets

Se crean tres datasets en la region `us-central1`, siguiendo el patron de capas de un Data Warehouse:

| Dataset | Proposito |
|---------|-----------|
| `dw_staging` | Datos crudos tal como llegan de la fuente |
| `dw_core` | Datos transformados y enriquecidos (tabla de hechos) |
| `dw_serving` | Vistas listas para consumo por analistas y herramientas de BI |

### 1.2 Carga del CSV

El archivo `ventas_2024.csv` se carga en `dw_staging.stg_ventas` con:
- **Deteccion automatica de esquema**
- **Particionada** por el campo `fecha` (tipo DATE)
- **Clusterizada** por el campo `region`

### 1.3 Tabla de hechos: `fact_ventas`

Se crea en `dw_core` aplicando transformaciones sobre `stg_ventas`:

```sql
ROUND(cantidad * precio_unitario * (1 - descuento_pct / 100), 2) AS total
```

- Mantiene todos los campos originales
- Agrega campo calculado `total` (precio neto con descuento)
- Agrega `_procesado_at` con timestamp de procesamiento
- Particionada por `fecha`, clusterizada por `region`

**Resultado por region:**

| Region   | Ventas | Total       |
|----------|--------|-------------|
| Arequipa | 9      | 4,200.00    |
| Lima     | 14     | 12,775.50   |
| Trujillo | 7      | 2,756.50    |

### 1.4 Vista: `v_resumen_ventas`

Vista en `dw_serving` que agrupa por `region`, `categoria` y `vendedor`:

| Campo | Descripcion |
|-------|-------------|
| `num_ventas` | Conteo de registros |
| `total_vendido` | Suma del campo total |
| `ticket_promedio` | Promedio del campo total (2 decimales) |

---

## Parte 2 — Control de Accesos

### 2.1 Service accounts

Se crean dos cuentas de servicio con el rol **BigQuery Job User** a nivel de proyecto (necesario para ejecutar consultas desde herramientas externas):

| Service Account | Rol | Proposito |
|-----------------|-----|-----------|
| `analista-ventas-sa` | BigQuery Job User | Simula un analista de ventas |
| `data-engineer-sa` | BigQuery Job User | Simula un data engineer |

### 2.2 Permisos por dataset

Los permisos a nivel de dataset siguen el **principio de minimo privilegio**: cada cuenta solo accede a lo que necesita para su funcion.

### 2.3 Validacion con Python

El script `parte2_validacion.py` verifica los permisos autenticandose con cada llave JSON:

```
--- analista-ventas-sa ---
  Leer dw_serving [DEBE FUNCIONAR]  →  EXITO
  Leer dw_staging [DEBE FALLAR]     →  ACCESO DENEGADO

--- data-engineer-sa ---
  Leer dw_staging [DEBE FUNCIONAR]  →  EXITO
  Leer dw_serving [DEBE FALLAR]     →  ACCESO DENEGADO
```

---

## Parte 3 — Seguridad a Nivel de Filas

### 3.1 Politicas RLS

Se crean dos politicas de Row-Level Security sobre `dw_core.fact_ventas`:

| Politica | Cuenta | Filtro | Efecto |
|----------|--------|--------|--------|
| `rls_lima` | analista-ventas-sa | `region = 'Lima'` | Solo ve ventas de Lima |
| `rls_admin` | data-engineer-sa | `TRUE` | Ve todas las regiones |

### 3.2 Verificacion del filtro

El script `parte3_rls.py` ejecuta el mismo query con ambas cuentas y compara resultados:

**analista-ventas-sa** (solo Lima):
| Region | Filas | Ventas |
|--------|-------|--------|
| Lima   | 14    | 12,775.50 |

**data-engineer-sa** (todas):
| Region   | Filas | Ventas    |
|----------|-------|-----------|
| Arequipa | 9     | 4,200.00  |
| Lima     | 14    | 12,775.50 |
| Trujillo | 7     | 2,756.50  |

---

## Requisitos

- **Python** 3.8 o superior
- **Google Cloud Platform** con un proyecto activo y BigQuery habilitado
- **gcloud CLI** instalado y autenticado
- Dos **service accounts** con sus llaves JSON (se generan en el Paso 2)

### Instalacion de dependencias

```bash
python -m venv venv
source venv/bin/activate
pip install google-cloud-bigquery db-dtypes pandas
```

### Autenticacion

```bash
gcloud auth application-default login
```

---

## Guia de ejecucion

Los scripts deben ejecutarse en orden ya que cada parte depende de la anterior:

```bash
# Activar entorno virtual
source venv/bin/activate

# Parte 1: Crear Data Warehouse (datasets + tablas + vista)
python parte1_data_warehouse.py

# Parte 2: Crear service accounts y asignar permisos
python parte2_control_accesos.py

# Parte 2: Validar que los permisos funcionan correctamente
python parte2_validacion.py

# Parte 3: Crear politicas RLS y verificar filtros
python parte3_rls.py
```

> **Nota:** Antes de ejecutar la Parte 2, asegurate de tener `gcloud` configurado con tu proyecto.

---

## Tecnologias utilizadas

| Tecnologia | Uso |
|------------|-----|
| **Google BigQuery** | Data Warehouse, almacenamiento y consultas |
| **Google Cloud IAM** | Control de accesos y service accounts |
| **BigQuery Row-Level Security** | Filtrado de datos por usuario |
| **Python 3** | Automatizacion de todo el pipeline |
| **google-cloud-bigquery** | SDK de Python para interactuar con BigQuery |
