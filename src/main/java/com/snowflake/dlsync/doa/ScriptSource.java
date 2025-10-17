package com.snowflake.dlsync.doa;

import com.snowflake.dlsync.ScriptFactory;
import com.snowflake.dlsync.models.*;
import com.snowflake.dlsync.parser.SqlTokenizer;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class ScriptSource {
    private String scriptRoot;
    private String mainScriptDir;
    private String testScriptDir;

    public ScriptSource(String scriptRoot) {
        this.scriptRoot = scriptRoot;
        mainScriptDir = Files.exists(Path.of(scriptRoot, "main")) ? Path.of(scriptRoot, "main").toString(): scriptRoot;
        testScriptDir = Path.of(scriptRoot, "test").toString();
        log.debug("Script file reader initialized with scriptRoot: {}", scriptRoot);
    }

    private List<String> readDatabase() {
        File scriptFiles = new File(mainScriptDir);
        List<String> dbs = new ArrayList<>();
        if(scriptFiles.exists()) {
            File[] allDbs = scriptFiles.listFiles();
            for(File file: allDbs) {
                if(file.isDirectory()) {
                    dbs.add(file.getName());
                }
            }
        }
        else {
            log.error("Invalid path for script provided: {}", scriptFiles.getAbsolutePath());
            throw new RuntimeException("No valid script source path provided");
        }
        return dbs;
    }

    private List<String> readSchemas(String database) {
        log.info("Reading all schema from database {}", database);
        List<String> schemas = new ArrayList<>();
        File dbFile = Path.of(mainScriptDir, database).toFile();
        if(dbFile.exists()) {
            File[] allFiles = dbFile.listFiles();
            for(File file: allFiles) {
                if(file.isDirectory()) {
                    schemas.add(file.getName());
                }
            }
        }
        return schemas;
    }

    public List<Script> getAllScripts() throws IOException {
        List<Script> allScripts = new ArrayList<>();
        for(String database: readDatabase()) {
            for(String schema: readSchemas(database)) {
                allScripts.addAll(getScriptsInSchema(database, schema));
            }
        }
        return allScripts;
    }

    public List<TestScript> getTestScripts(List<Script> scripts) throws IOException {
            List<TestScript> testScripts = scripts.stream()
                    .map(script -> {
                        try {
                            return getTestScript(script);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .filter(testScript -> testScript != null)
                    .collect(Collectors.toList());
            return testScripts;
    }

    public List<Script> getScriptsInSchema(String database, String schema) throws IOException {
        log.info("Reading script files from schema: {}", schema);
        List<Script> scripts = new ArrayList<>();
        File schemaDirectory = Path.of(mainScriptDir, database, schema).toFile();
        File[] scriptTypeDirectories = schemaDirectory.listFiles();

        for(File scriptType: scriptTypeDirectories) {
            if(scriptType.isDirectory() ) {
                File[] scriptFiles = scriptType.listFiles();
                for(File file: scriptFiles) {
                    if(file.getName().toLowerCase().endsWith(".sql")){
                       String fileName = file.getName().replace("'", "\"");
                       Set<Script> scriptsFromFile = SqlTokenizer.parseScript(file.getPath(), fileName, scriptType.getName(), Files.readString(file.toPath()));
                       scripts.addAll(scriptsFromFile);
                    }
                    else {
                        log.warn("Script Skipped, File not SQL: {} ", file.getName());
                    }
                }
            }
            else {
                log.warn("Script file found outside object type directory: {} ", scriptType.getName());
            }
        }
        return scripts;
    }

    public Set<Script> buildScriptFromFile(File file, File scriptType) throws IOException {
        String content = Files.readString(file.toPath());
        String fileName = file.getName().replace("'", "\"");
        String objectName = SqlTokenizer.extractObjectName(fileName, content);
        ScriptObjectType objectType = ScriptObjectType.valueOf(scriptType.getName());
        String fullIdentifier = SqlTokenizer.getFirstFullIdentifier(objectName, content);
        if(fullIdentifier == null || fullIdentifier.isEmpty()) {
            log.error("Error reading script: {}, name and content mismatch", file.getName());
            throw new RuntimeException("Object name and file name must match!");
        }
        String database = SqlTokenizer.extractDatabaseName(fullIdentifier);
        String schema = SqlTokenizer.extractSchemaName(fullIdentifier);
        if(database == null || schema == null) {
            log.error("Error reading script: {}, database or schema not specified", file.getName());
            throw new RuntimeException("Database, schema and object name must be provided in the script file.");
        }
        Set<Script> scripts = new HashSet<>();
        if(objectType.isMigration()) {
            List<Migration> migrations = SqlTokenizer.parseMigrationScripts(content);
            for(Migration migration: migrations) {
                MigrationScript script = ScriptFactory.getMigrationScript(database, schema, objectType, objectName, migration);
//                Script script = new Script(database, schema, objectType, objectName, migration.getContent(), migration.getVersion(), migration.getAuthor(), migration.getRollback());
                if(scripts.contains(script)) {
                    log.error("Duplicate version {} for script {} found.", script.getVersion(), script);
                    throw new RuntimeException("Duplicate version number is not allowed in the same script file.");
                }
                scripts.add(script);
            }
        }
        else {
            Script script = ScriptFactory.getStateScript(file.getPath(), database, schema, objectType, objectName, content);
//            Script script = new Script(database, schema, objectType, objectName, content);
            scripts.add(script);
        }
        return scripts;
    }


    public TestScript getTestScript(Script script) throws IOException {
        String objectName = script.getObjectName() + "_TEST";
        String testScriptPath = script.getScriptPath().replace(".SQL", "_TEST.SQL");
        testScriptPath = testScriptPath.replaceAll("^" + mainScriptDir, testScriptDir);
        File file = Path.of(testScriptPath).toFile();
        if(file.exists()) {
            log.info("Test script file found: {}", file.getPath());
            String content = Files.readString(file.toPath());
            TestScript testScript = ScriptFactory.getTestScript(file.getPath(), script.getDatabaseName(), script.getSchemaName(), script.getObjectType(), objectName, content, script);
            return testScript;
        }
        return null;

    }

    public void createScriptFiles(List<Script> scripts) {
        log.debug("Creating script files for the scripts: {}", scripts);
        for(Script script: scripts) {
            createScriptFile(script);
        }
    }

    public void createScriptFile(Script script) {
        try {
            String scriptFileName = script.getObjectName() + ".SQL";
            scriptFileName = scriptFileName.replace("\"", "'");
            String scriptDirectoryPath = String.format("%s/%s/%s/%s", mainScriptDir, script.getDatabaseName(), script.getSchemaName(), script.getObjectType());
            File directory = new File(scriptDirectoryPath);
            directory.mkdirs();
            FileWriter fileWriter = new FileWriter(Path.of(scriptDirectoryPath,  scriptFileName).toFile());
            fileWriter.write(script.getContent());
            fileWriter.close();
            log.debug("File {} created successfully", Path.of(scriptDirectoryPath,  scriptFileName));
        } catch (IOException e) {
            log.error("Error in creating script: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private Script getScriptByName(List<Script> allScripts, String fullObjectName) {
        return allScripts.parallelStream().filter(script -> script.getFullObjectName().equals(fullObjectName)).findFirst().get();
    }
}

