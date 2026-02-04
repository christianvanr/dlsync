CREATE OR REPLACE CORTEX SEARCH SERVICE ${EXAMPLE_DB}.${MAIN_SCHEMA}.PRODUCT_SEARCH
  ON product_description
  ATTRIBUTES product_name, category
  WAREHOUSE = ${MY_WAREHOUSE}
  TARGET_LAG = '1 hour'
AS (
  SELECT
    product_name,
    product_description,
    category
  FROM ${EXAMPLE_DB}.${MAIN_SCHEMA}.PRODUCTS
);
