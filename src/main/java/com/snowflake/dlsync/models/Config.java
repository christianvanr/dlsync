package com.snowflake.dlsync.models;

import lombok.Data;

import java.util.List;

@Data
public class Config {
    private String version;
    private List<String> scriptExclusion;
    private List<DependencyOverride> dependencyOverride;
    private List<String> configTables;

    public boolean isScriptExcluded(Script script) {
        if(scriptExclusion == null) {
            return false;
        }
        return scriptExclusion.contains(script.getFullObjectName());
    }
}
