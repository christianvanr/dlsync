package com.snowflake.dlsync;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.snowflake.dlsync.models.Config;
import com.snowflake.dlsync.models.Script;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class ConfigManager {
    private String scriptRoot;
    private Config config;
    private final static String CONFIG_FILE_NAME = "config.yaml";
    public ConfigManager(String scriptRoot) {
        this.scriptRoot = scriptRoot;
    }

    public void readConfig() throws IOException {
        File configFile = Path.of(scriptRoot, CONFIG_FILE_NAME).toFile();
        System.out.println(configFile.getAbsolutePath());
        if(configFile.exists()) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.findAndRegisterModules();
            config = mapper.readValue(configFile, Config.class);
        }
        else {
            config = new Config();
        }

    }

    public Config getConfig() throws IOException {
        if(config == null) {
            readConfig();
        }
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public boolean isScriptExcluded(Script script) {
        if(config == null || config.getScriptExclusion() == null) {
            return false;
        }
        return config.getScriptExclusion().contains(script.getFullObjectName());
    }
}
