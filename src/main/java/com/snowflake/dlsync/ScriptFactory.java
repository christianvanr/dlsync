package com.snowflake.dlsync;

import com.snowflake.dlsync.models.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ScriptFactory {
    public static StateScript getStateScript(String databaseName, String schemaName, ScriptObjectType objectType, String objectName, String content) {
        return new StateScript(databaseName, schemaName, objectName, objectType, content);
    }

    public static MigrationScript getMigrationScript(String databaseName, String schemaName, ScriptObjectType objectType, String objectName, String content, Long version, String author, String rollback, String verify) {
        return new MigrationScript(databaseName, schemaName, objectName, objectType, content, version, author, rollback, verify);
    }

    public static MigrationScript getMigrationScript(String fullObjectName, ScriptObjectType objectType, String content, Long version, String author, String rollback, String verify) {
        String databaseName = null, schemaName = null, objectName = null;
        String[] nameSplit = fullObjectName.split("\\.");
        if(nameSplit.length > 2) {
            databaseName = nameSplit[0];
            schemaName = nameSplit[1];
            objectName = nameSplit[2];
        }
        else {
            log.error("Error while splitting fullObjectName {}: Missing some values", fullObjectName);
            throw new RuntimeException("Error while splitting fullObjectName");
        }
        return new MigrationScript(databaseName, schemaName, objectName, objectType, content, version, author, rollback, verify);
    }
    public static MigrationScript getMigrationScript(String database, String schema, ScriptObjectType type, String objectName, String content) {
        Long version = 0L;
        String author = "DlSync";
        String rollback = String.format("DROP %s IF EXISTS %s;", type.getSingular(), database + "." + schema + "." + objectName);
        String verify = String.format("SHOW %s LIKE '%s';",type, database + "." + schema + "." + objectName);

        String migrationHeader = String.format("---version: %s, author: %s\n", version, author);
        String rollbackFormat = String.format("\n---rollback: %s", rollback);
        String verifyFormat = String.format("\n---verify: %s", verify);
        content = migrationHeader + content + rollbackFormat + verifyFormat;
        MigrationScript script = getMigrationScript(database, schema, type, objectName, content, version, author, rollback, verify);
        return script;
    }

    public static MigrationScript getMigrationScript(String databaseName, String schemaName, ScriptObjectType objectType, String objectName, Migration migration) {
        return new MigrationScript(databaseName, schemaName, objectName, objectType, migration.getContent(), migration.getVersion(), migration.getAuthor(), migration.getRollback(), migration.getVerify());
    }

}
