package com.snowflake.dlsync.models;

import lombok.Data;

import java.util.List;

@Data
public class DependencyOverride {
    private String script;
    private List<String> dependencies;
}