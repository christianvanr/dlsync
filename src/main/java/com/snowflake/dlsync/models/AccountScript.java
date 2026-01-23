package com.snowflake.dlsync.models;

import java.util.List;
import java.util.Objects;

public class AccountScript extends Script {

    public AccountScript(String scriptPath, String objectName, ScriptObjectType objectType, String content) {
        super(scriptPath, objectName, objectType, content);
    }

    public AccountScript(String scriptPath, String objectName, ScriptObjectType objectType, String content, List<MigrationScript> migrations) {
        super(scriptPath, objectName, objectType, content, migrations);
    }

    @Override
    public String getId() {
        return getFullObjectName();
    }

    @Override
    public String getFullObjectName() {
        return getObjectName();
    }

    @Override
    public String resolveObjectReference(String partialName) {
        return partialName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode());
    }
}
