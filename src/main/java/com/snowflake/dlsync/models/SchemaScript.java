package com.snowflake.dlsync.models;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;

@Slf4j
public class SchemaScript extends Script {
    private String databaseName;
    private String schemaName;

    public SchemaScript(String scriptPath, String databaseName, String schemaName, String objectName, ScriptObjectType objectType, String content) {
        super(scriptPath, objectName, objectType, content);
        this.databaseName = databaseName.toUpperCase();
        this.schemaName = schemaName.toUpperCase();
    }

    public SchemaScript(String scriptPath, String databaseName, String schemaName, String objectName, ScriptObjectType objectType, String content, List<MigrationScript> migrations) {
        super(scriptPath, objectName, objectType, content, migrations);
        this.databaseName = databaseName.toUpperCase();
        this.schemaName = schemaName.toUpperCase();
    }

    @Override
    public String getId() {
        return getFullObjectName();
    }

    @Override
    public String getFullObjectName() {
        return String.format("%s.%s.%s", databaseName, schemaName, getObjectName());
    }

    @Override
    public String resolveObjectReference(String partialName) {
        String[] objectHierarchy = partialName.split("\\.");
        String fullyQualifiedName = "%s.%s.%s";
        if(objectHierarchy.length == 1) {
            fullyQualifiedName = String.format("%s.%s.%s", databaseName, schemaName,  objectHierarchy[0]);
        }
        else if(objectHierarchy.length == 2) {
            fullyQualifiedName = String.format("%s.%s.%s", databaseName, objectHierarchy[0],  objectHierarchy[1]);
        }
        else if(objectHierarchy.length == 3) {
            fullyQualifiedName = String.format("%s.%s.%s", objectHierarchy[0], objectHierarchy[1],  objectHierarchy[2]);
        }
        else {
            log.error("Unknown dependency extracted: {} from script: {}", partialName, this);
        }
        return fullyQualifiedName.toUpperCase();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName.toUpperCase();
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName.toUpperCase();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), databaseName, schemaName);
    }

}

