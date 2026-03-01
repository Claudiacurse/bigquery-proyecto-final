# Trabajo Final — BigQuery for Analytics

Proyecto final del módulo BigQuery for Analytics del Diplomado en Data Analytics.

## Descripción

Construcción de un Data Warehouse en BigQuery con tres capas (staging, core, serving), control de accesos mediante service accounts y seguridad a nivel de filas (Row-Level Security).

## Estructura del proyecto

| Archivo | Descripción |
|---------|-------------|
| `parte1_data_warehouse.py` | Creación de datasets, carga de CSV, tabla de hechos y vista de resumen |
| `parte2_control_accesos.py` | Creación de service accounts y asignación de permisos por dataset |
| `parte2_validacion.py` | Validación de permisos (EXITO / ACCESO DENEGADO) |
| `parte3_rls.py` | Políticas RLS y verificación del filtro por región |
| `queries.sql` | Todos los queries SQL utilizados |
| `ventas_2024.csv` | Dataset de ventas (30 registros) |

## Arquitectura

```
ventas_2024.csv
      │
      ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ dw_staging   │────▶│  dw_core     │────▶│ dw_serving   │
│ stg_ventas   │     │ fact_ventas  │     │ v_resumen    │
└─────────────┘     └─────────────┘     └─────────────┘
                           │
                     Row-Level Security
                     ├─ rls_lima (solo Lima)
                     └─ rls_admin (todas)
```

## Requisitos

- Python 3.8+
- Google Cloud Platform con BigQuery habilitado
- Paquetes: `google-cloud-bigquery`, `db-dtypes`, `pandas`

```bash
pip install google-cloud-bigquery db-dtypes pandas
```

## Ejecución

```bash
# 1. Data Warehouse
python parte1_data_warehouse.py

# 2. Control de accesos
python parte2_control_accesos.py
python parte2_validacion.py

# 3. Row-Level Security
python parte3_rls.py
```

## Tecnologías

- Google BigQuery
- Python 3
- Google Cloud IAM
