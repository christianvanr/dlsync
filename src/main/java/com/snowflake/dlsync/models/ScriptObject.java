package com.snowflake.dlsync.models;

import com.snowflake.dlsync.Util;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
public class ScriptObject {
    private String databaseName;
    private String schemaName;
    private String objectName;
    private ScriptObjectType objectType;
    private Long version;
    private String author;
    private String content;
    private String rollback;
    private String hash;

    public ScriptObject(String databaseName, String schemaName, ScriptObjectType objectType, String objectName, String content) {
        this(databaseName, schemaName, objectType, objectName, content, null, null, null);
    }

    public ScriptObject(String databaseName, String schemaName, ScriptObjectType objectType, String objectName, String content, Long version, String author, String rollback) {
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.objectType = objectType;
        this.objectName = objectName;
        this.content = content.trim();
        this.version = version;
        this.author = author;
        this.rollback = rollback;
        hash = Util.getMd5Hash(this.content);
        log.debug("Loaded script for {}", this.getObjectName());
    }


    public String getObjectName() {
        return objectName.toUpperCase();
    }
    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getSchemaName() {
        return schemaName.toUpperCase();
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName.toUpperCase();
    }

    public ScriptObjectType getObjectType() {
        return objectType;
    }

    public Long getVersion() {
        return version;
    }

    public String getAuthor() {
        return author;
    }

    public String getRollback() {
        return rollback;
    }

    public String getHash() {
        return hash;
    }
    public String getFullObjectName() {
        return String.format("%s.%s.%s", getDatabaseName(), getSchemaName(), getObjectName()).toUpperCase();
    }

    public String getIdentifier() {
        return getFullObjectName() + ":" + version;
    }

    public boolean isMigration() {
        return objectType == ScriptObjectType.TABLES;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScriptObject script = (ScriptObject) o;
        return Objects.equals(databaseName, script.databaseName) && Objects.equals(schemaName, script.schemaName) && Objects.equals(objectName, script.objectName) && Objects.equals(version, script.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, schemaName, objectName, version);
    }

    @Override
    public String toString() {
        return getFullObjectName() + ":" + version;
    }
}
