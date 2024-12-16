package com.snowflake.dlsync.doa;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.snowflake.dlsync.models.Config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class ConfigRepo {
    private String scriptRoot;
    private Config config;
    private final static String CONFIG_FILE_NAME = "config.yaml";
    public ConfigRepo(String scriptRoot) throws IOException {
        this.scriptRoot = scriptRoot;
        config = new Config();
        readConfig();
    }

    private void readConfig() throws IOException {
        File configFile = Path.of(scriptRoot, CONFIG_FILE_NAME).toFile();
        System.out.println(configFile.getAbsolutePath());
        if(configFile.exists()) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.findAndRegisterModules();
            config = mapper.readValue(configFile, Config.class);
        }

    }

    public Config getConfig() {
        return config;
    }
}
