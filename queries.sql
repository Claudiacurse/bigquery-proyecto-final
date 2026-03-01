-- ============================================================
-- TRABAJO FINAL — BigQuery for Analytics
-- Proyecto: clau-personal
-- ============================================================

-- ============================================================
-- PARTE 1 — Preparación del Data Warehouse
-- ============================================================

-- 1.1 Creación de datasets (ejecutar en BigQuery o con bq CLI)
-- CREATE SCHEMA `clau-personal.dw_staging` OPTIONS(location='us-central1');
-- CREATE SCHEMA `clau-personal.dw_core` OPTIONS(location='us-central1');
-- CREATE SCHEMA `clau-personal.dw_serving` OPTIONS(location='us-central1');

-- 1.3 Creación de fact_ventas en dw_core
CREATE OR REPLACE TABLE `clau-personal.dw_core.fact_ventas`
PARTITION BY fecha
CLUSTER BY region
AS
SELECT
    id_venta,
    fecha,
    region,
    categoria,
    producto,
    cantidad,
    precio_unitario,
    descuento_pct,
    vendedor,
    ROUND(cantidad * precio_unitario * (1 - descuento_pct / 100), 2) AS total,
    CURRENT_TIMESTAMP() AS _procesado_at
FROM `clau-personal.dw_staging.stg_ventas`;

-- Evidencia 1.3
SELECT region, COUNT(*) AS ventas, ROUND(SUM(total),2) AS total_ventas
FROM `clau-personal.dw_core.fact_ventas`
GROUP BY region
ORDER BY region;

-- 1.4 Creación de vista v_resumen_ventas
CREATE OR REPLACE VIEW `clau-personal.dw_serving.v_resumen_ventas` AS
SELECT
    region,
    categoria,
    vendedor,
    COUNT(*) AS num_ventas,
    ROUND(SUM(total), 2) AS total_vendido,
    ROUND(AVG(total), 2) AS ticket_promedio
FROM `clau-personal.dw_core.fact_ventas`
GROUP BY region, categoria, vendedor;

-- Evidencia 1.4
SELECT * FROM `clau-personal.dw_serving.v_resumen_ventas`
ORDER BY total_vendido DESC
LIMIT 5;

-- ============================================================
-- PARTE 3 — Row-Level Security
-- ============================================================

-- 3.1 Políticas RLS
CREATE OR REPLACE ROW ACCESS POLICY rls_lima
  ON `clau-personal.dw_core.fact_ventas`
  GRANT TO ('serviceAccount:analista-ventas-sa@clau-personal.iam.gserviceaccount.com')
  FILTER USING (region = 'Lima');

CREATE OR REPLACE ROW ACCESS POLICY rls_admin
  ON `clau-personal.dw_core.fact_ventas`
  GRANT TO ('serviceAccount:data-engineer-sa@clau-personal.iam.gserviceaccount.com')
  FILTER USING (TRUE);

-- Evidencia 3.1 (ejecutar en la consola de BigQuery)
SELECT
    policy_id,
    filter_predicate,
    ARRAY_TO_STRING(grantee_list, ', ') AS aplica_a
FROM `clau-personal`.dw_core.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES
WHERE table_name = 'fact_ventas';
