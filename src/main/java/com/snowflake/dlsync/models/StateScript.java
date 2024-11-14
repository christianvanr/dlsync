package com.snowflake.dlsync.models;

public class StateScript extends Script {


    public StateScript(String databaseName, String schemaName, String objectName, ScriptObjectType objectType, String content) {
        super(databaseName, schemaName, objectName, objectType, content);
    }

    @Override
    public String getId() {
        return getFullObjectName();
    }
}
