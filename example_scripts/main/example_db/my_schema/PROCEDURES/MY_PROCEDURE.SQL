CREATE OR REPLACE PROCEDURE ${EXAMPLE_DB}.${MY_SCHEMA}.MY_PROCEDURE("ID" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
        var response = {"userId": null, "uuid": null};
        var userId = ID;
        if (null == userId) {
            var userStmt = snowflake.createStatement({sqlText: "select current_user"});
            var userRs = userStmt.execute();
            userRs.next();
            userId = userRs.getColumnValue(1);
}
        var uuidStmt = snowflake.createStatement({sqlText: "SELECT UUID_STRING()"});
        var uuidRs  = uuidStmt.execute();
        uuidRs.next();
        var uuid = uuidRs.getColumnValue(1);
        response = {"userId": userId, "uuid": uuid};
return response;
';