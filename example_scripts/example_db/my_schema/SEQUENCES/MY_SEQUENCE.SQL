---version: 0, author: DlSync
create or replace sequence ${EXAMPLE_DB}.${MY_SCHEMA}.MY_SEQUENCE start with 1 increment by 1;
---rollback: DROP SEQUENCE IF EXISTS ${EXAMPLE_DB}.${MY_SCHEMA}.MY_SEQUENCE;
---verify: SHOW SEQUENCES LIKE '${EXAMPLE_DB}.${MY_SCHEMA}.MY_SEQUENCE';