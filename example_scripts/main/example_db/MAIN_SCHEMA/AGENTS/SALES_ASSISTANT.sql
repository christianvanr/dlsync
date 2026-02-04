CREATE OR REPLACE AGENT ${EXAMPLE_DB}.${MAIN_SCHEMA}.SALES_ASSISTANT
  COMMENT = 'Sales assistant agent for product and order queries'
  PROFILE = '{"display_name": "Sales Assistant", "avatar": "sales-icon.png", "color": "blue"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: claude-4-sonnet

  orchestration:
    budget:
      seconds: 30
      tokens: 16000

  instructions:
    response: "You will respond in a friendly but professional manner about sales and products"
    orchestration: "For product questions use Search; for sales analytics use Analyst"
    system: "You are a helpful sales assistant that helps with product and order inquiries"
    sample_questions:
      - question: "What products do we have in stock?"
        answer: "I'll search our product catalog to find available items."
      - question: "What were our top selling products last month?"
        answer: "I'll analyze our sales data to find the best performers."

  tools:
    - tool_spec:
        type: "cortex_analyst_text_to_sql"
        name: "SalesAnalyst"
        description: "Analyzes sales data and generates reports"
    - tool_spec:
        type: "cortex_search"
        name: "ProductSearch"
        description: "Searches product catalog and documentation"

  tool_resources:
    SalesAnalyst:
      semantic_view: "${EXAMPLE_DB}.${MAIN_SCHEMA}.SALES_ANALYTICS"
    ProductSearch:
      name: "${EXAMPLE_DB}.${MAIN_SCHEMA}.PRODUCT_SEARCH"
      max_results: "10"
  $$;
