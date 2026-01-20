CREATE OR REPLACE NETWORK POLICY allow_internal_ips
  ALLOWED_NETWORK_RULE_LIST = (${EXAMPLE_DB}.${MAIN_SCHEMA}.INTERNAL_IPS_RULE)
  COMMENT = 'Network policy for internal access only';
