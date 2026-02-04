CREATE OR REPLACE SEMANTIC VIEW ${EXAMPLE_DB}.${MAIN_SCHEMA}.SALES_ANALYTICS
  TABLES (
    products AS ${EXAMPLE_DB}.${MAIN_SCHEMA}.PRODUCTS
      PRIMARY KEY (PRODUCT_ID)
      COMMENT = 'Product catalog table',
    orders AS ${EXAMPLE_DB}.${MAIN_SCHEMA}.ORDERS
      PRIMARY KEY (ORDER_ID)
      COMMENT = 'Customer orders table'
  )
  RELATIONSHIPS (
    orders (PRODUCT_ID) REFERENCES products
  )
  DIMENSIONS (
    products.product_name AS products.NAME
      WITH SYNONYMS = ('product', 'item')
      COMMENT = 'Name of the product',
    products.category AS products.CATEGORY
      WITH SYNONYMS = ('type', 'group')
      COMMENT = 'Product category',
    orders.order_date AS orders.ORDER_DATE
      WITH SYNONYMS = ('date', 'purchase date')
      COMMENT = 'Date when order was placed'
  )
  METRICS (
    orders.total_revenue AS SUM(orders.PRICE * orders.QUANTITY)
      WITH SYNONYMS = ('revenue', 'sales')
      COMMENT = 'Total revenue from orders',
    orders.order_count AS COUNT(orders.ORDER_ID)
      WITH SYNONYMS = ('number of orders', 'count')
      COMMENT = 'Total number of orders'
  )
  COMMENT = 'Semantic view for sales analytics'
  AI_SQL_GENERATION 'When asked about sales, use total_revenue metric. When asked about orders, use order_count metric.';
