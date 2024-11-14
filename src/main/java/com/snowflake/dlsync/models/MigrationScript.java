package com.snowflake.dlsync.models;

public class MigrationScript extends Script {

    private Long version;
    private String author;
    private String rollback;
    private String verify;

    public MigrationScript(String databaseName, String schemaName, String objectName, ScriptObjectType objectType, String content, Long version, String author, String rollback, String verify) {
        super(databaseName, schemaName, objectName, objectType, content);
        this.version = version;
        this.author = author;
        this.rollback = rollback;
        this.verify = verify;
    }

    @Override
    public String getId() {
        return String.format("%s:%s", getFullObjectName(), version);
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getRollback() {
        return rollback;
    }

    public void setRollback(String rollback) {
        this.rollback = rollback;
    }

    public String getVerify() {
        return verify;
    }

    public void setVerify(String verify) {
        this.verify = verify;
    }
}
