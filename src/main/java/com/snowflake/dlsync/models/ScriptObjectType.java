package com.snowflake.dlsync.models;

public enum ScriptObjectType {
    VIEWS("VIEW"),FUNCTIONS("FUNCTION"),PROCEDURES("PROCEDURE"),STREAMS("STREAM"),TASKS("TASK"),SEQUENCES("SEQUENCE"),STAGES("STAGE"),TABLES("TABLE"), FILE_FORMATS("FILE FORMAT");

    private final String singular;
    private ScriptObjectType(String type) {
        this.singular = type;
    }
    public String getSingular() {
        return singular;
    }
    public boolean isMigration() {
        switch (this) {
            case TABLES:
            case STREAMS:
            case SEQUENCES:
            case STAGES:
            case TASKS:
                return true;
            default:
                return false;
        }
    }
}
