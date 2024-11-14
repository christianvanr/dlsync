

package com.snowflake.dlsync.models;

import com.snowflake.dlsync.Util;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
public abstract class Script {
    private String databaseName;
    private String schemaName;
    private String objectName;
    private ScriptObjectType objectType;
    private String content;
    private String hash;

    public Script(String databaseName, String schemaName, String objectName, ScriptObjectType objectType, String content) {
        this.databaseName = databaseName.toUpperCase();
        this.schemaName = schemaName.toUpperCase();
        this.objectName = objectName.toUpperCase();
        this.objectType = objectType;
        this.content = content.trim();
        this.hash = hash = Util.getMd5Hash(this.content);
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

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName.toUpperCase();
    }

    public ScriptObjectType getObjectType() {
        return objectType;
    }

    public void setObjectType(ScriptObjectType objectType) {
        this.objectType = objectType;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content.trim();
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public String getFullObjectName() {
        return String.format("%s.%s.%s", databaseName, schemaName, objectName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Script script = (Script) o;
        return Objects.equals(getObjectType(), script.getObjectType()) && Objects.equals(getId(), script.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
    }

    @Override
    public String toString() {
        return getId();
    }
    public abstract String getId();

}
