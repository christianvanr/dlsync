package com.snowflake.dlsync.models;

import lombok.Data;

import java.sql.Date;
import java.util.Objects;

@Data
public class ScriptDependency {
    private String objectName;
    private ScriptObjectType objectType;
    private String dependency;
    private ScriptObjectType dependencyObjectType;
    private String createdBy;
    private Date createdTs;

    public ScriptDependency(Script node, Script dependency) {
        this.objectName = node.getObjectName();
        this.objectType = node.getObjectType();
        this.dependency = dependency.getObjectName();
        this.dependencyObjectType = dependency.getObjectType();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScriptDependency scriptDependency = (ScriptDependency) o;
        return Objects.equals(getObjectName(), scriptDependency.getObjectName()) && Objects.equals(getDependency(), scriptDependency.getDependency());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getObjectName(), getDependency());
    }
}
