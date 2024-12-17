package com.snowflake.dlsync;

import com.snowflake.dlsync.dependency.DependencyExtractor;
import com.snowflake.dlsync.dependency.DependencyGraph;
import com.snowflake.dlsync.doa.ScriptRepo;
import com.snowflake.dlsync.doa.ScriptSource;
import com.snowflake.dlsync.models.*;
import com.snowflake.dlsync.parser.ParameterInjector;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class ChangeManager {
    private final static String[] JDBC_KEY = {"user", "password", "account", "warehouse", "db", "schema", "role", "authenticator"};
    private final static String SCRIPT_ROOT_KEY = "SCRIPT_ROOT";
    private String profile;
    private Properties scriptParameters;
    private Properties jdbcProperties;
    private String scriptRoot;

    private ConfigManager configManager;
    private ScriptSource scriptSource;
    private ScriptRepo scriptRepo;
    private DependencyGraph dependencyGraph;
    private DependencyExtractor dependencyExtractor;
    private ParameterInjector parameterInjector;

    public ChangeManager() throws IOException, NoSuchAlgorithmException, SQLException {
        jdbcProperties = new Properties();
        loadEnvProperties();
        scriptParameters = new Properties();
        loadParameters();
        log.info("Initialized with the following parameters: {}", scriptParameters);
        configManager = new ConfigManager(scriptRoot);
        Config config = configManager.getConfig();
        scriptRepo = new ScriptRepo(jdbcProperties);
        scriptSource = new ScriptSource(scriptRoot);
        parameterInjector = new ParameterInjector(scriptParameters);
        dependencyExtractor = new DependencyExtractor();
        dependencyGraph = new DependencyGraph(dependencyExtractor, config);
    }


    private void loadEnvProperties() {
        profile = StringUtils.isEmpty(System.getenv("profile")) ? "dev" : System.getenv("profile").toLowerCase();
        scriptRoot = System.getenv(SCRIPT_ROOT_KEY);
        log.info("Using {} profile and script root path of {}", profile, scriptRoot);
        for(String key: JDBC_KEY) {
            String jdbcConfigValue = System.getenv(key);
            if(jdbcConfigValue == null) {
                if(!key.equals("authenticator")) {
                    log.warn("JDBC connection property {} not found", key);
                }
            }
            else {
                jdbcProperties.put(key, jdbcConfigValue);
            }
        }
    }

    private void loadParameters() throws IOException {
        String propertiesFilePath = "parameter-" + profile + ".properties";
        log.debug("Loading property file from {}", propertiesFilePath);
        InputStream input = new FileInputStream(Path.of(scriptRoot, propertiesFilePath).toFile());
        scriptParameters.load(input);
        Map<String, String> environmentVariables = System.getenv();
        for(String key: environmentVariables.keySet()) {
            if(scriptParameters.containsKey(key)) {
                scriptParameters.put(key, environmentVariables.get(key));
            }
        }
    }

    private void validateScript(Script script) {
        if(script instanceof MigrationScript && scriptRepo.isScriptVersionDeployed(script)) {
            log.error("Migration type script changed. Script for the object {} has changed from previous deployments.", script.getId());
            throw new RuntimeException("Migration type scripts should not change.");
        }
    }
    public void deploy(boolean onlyHashes) throws SQLException, IOException, NoSuchAlgorithmException{
        log.info("Started Deploying {}", onlyHashes?"Only Hashes":"scripts");
        startSync(ChangeType.DEPLOY);
        scriptRepo.loadScriptHash();
        List<Script> changedScripts = scriptSource.getAllScripts()
                .stream()
                .filter(script -> !configManager.isScriptExcluded(script))
                .filter(script -> scriptRepo.isScriptChanged(script))
                .collect(Collectors.toList());
        dependencyGraph.addNodes(changedScripts);
        List<Script> sequencedScript = dependencyGraph.topologicalSort();
        log.info("Deploying {} change scripts to db.", sequencedScript.size());
        int size = sequencedScript.size();
        int index = 1;
        for(Script script: sequencedScript) {
            log.info("{} of {}: Deploying object: {}", index++, size, script);
            parameterInjector.injectParameters(script);
            validateScript(script);
            scriptRepo.createScriptObject(script, onlyHashes);
        }
        endSyncSuccess(ChangeType.DEPLOY, (long)sequencedScript.size());
    }

    public void rollback() throws SQLException, IOException {
        log.info("Starting ROLLBACK scripts.");
        startSync(ChangeType.ROLLBACK);
        Set<String> deployedScriptIds = scriptRepo.loadScriptHash();
        scriptSource.getAllScripts().forEach(script -> deployedScriptIds.remove(script.getId()));
        List<MigrationScript> migrations = scriptRepo.getMigrationScripts(deployedScriptIds);

        dependencyGraph.addNodes(migrations);
        List<Script> sequencedScript = dependencyGraph.topologicalSort();

        int size = sequencedScript.size();
        int index = 1;
        for(int i = sequencedScript.size() - 1; i >= 0; i--) {
            MigrationScript migration = (MigrationScript) sequencedScript.get(i);
            log.info("{} of {}: Rolling-back object: {}", index++, size, migration);
            parameterInjector.injectParametersAll(migration);
            scriptRepo.executeRollback(migration);
        }
        endSyncSuccess(ChangeType.ROLLBACK, 0L);
    }

    public boolean verify() throws IOException, NoSuchAlgorithmException, SQLException{
        log.info("Started verify scripts.");
        startSync(ChangeType.VERIFY);
        scriptRepo.loadDeployedHash();
        int failedCount = 0;
        List<Script> allScripts = scriptSource.getAllScripts().stream()
                .filter(script -> !configManager.isScriptExcluded(script))
                .collect(Collectors.toList());
        Map<String, List<MigrationScript>> groupedMigrationScripts = allScripts.stream()
                .filter(script -> script instanceof MigrationScript)
                .map(script -> (MigrationScript)script)
                .collect(Collectors.groupingBy(Script::getObjectName));

        for(String objectName: groupedMigrationScripts.keySet()) {
            List<MigrationScript> sameObjectMigrations = groupedMigrationScripts.get(objectName);
            Optional<MigrationScript> lastMigration = sameObjectMigrations.stream().sorted(Comparator.comparing(MigrationScript::getVersion).reversed()).findFirst();
            if (lastMigration.isPresent()) {
                MigrationScript migrationScript = lastMigration.get();
                parameterInjector.injectParametersAll(migrationScript);
                if (!scriptRepo.executeVerify(migrationScript)) {
                    failedCount++;
                    log.error("Script verification failed for {}", migrationScript);
                } else {
                    log.info("Verified Script {} is correct.", migrationScript);
                }
            }
        }

        List<String> schemaNames = scriptRepo.getAllSchemasInDatabase(scriptRepo.getDatabaseName());
        for(String schema: schemaNames) {
            List<Script> stateScripts = scriptRepo.getStateScriptsInSchema(schema);
            for(Script script: stateScripts) {
                parameterInjector.parametrizeScript(script, true);
                if(configManager.isScriptExcluded(script)) {
                    log.info("Script {} is excluded from verification.", script);
                    continue;
                }
                if (!scriptRepo.verifyScript(script)) {
                    failedCount++;
                    log.error("Script verification failed for {}", script);
                } else {
                    log.info("Verified Script {} is correct.", script);
                }
            }
        }

        if(failedCount != 0) {
            log.error("Verification failed!");
            endSyncError(ChangeType.VERIFY, failedCount + " scripts failed to verify.");
            throw new RuntimeException("Verification failed!");
        }
        log.info("All scripts have been verified successfully.");
        endSyncSuccess(ChangeType.VERIFY, (long)allScripts.size());
        return true;
    }

    public void createAllScriptsFromDB(List<String> schemaNames) throws SQLException, IOException {
        log.info("Started create scripts from database with {} schemas", schemaNames == null ? "All":schemaNames);
        startSync(ChangeType.CREATE_SCRIPT);
        Config config = configManager.getConfig();
        HashSet<String> configTableWithParameter = new HashSet<>();
        if(config != null && config.getConfigTables() != null) {
            configTableWithParameter.addAll(config.getConfigTables());
        }
        Set<String> configTables = parameterInjector.injectParameters(configTableWithParameter);

        if(schemaNames == null) {
            schemaNames = scriptRepo.getAllSchemasInDatabase(scriptRepo.getDatabaseName());
        }
        int count = 0;
        for(String schema: schemaNames) {
            List<Script> scripts = scriptRepo.getAllScriptsInSchema(schema);
            for(Script script: scripts) {
                count++;
                if(configTables.contains(script.getFullObjectName())) {
                    scriptRepo.addConfig(script);
                }
                parameterInjector.parametrizeScript(script, false);
            }
            scriptSource.createScriptFiles(scripts);
        }
        endSyncSuccess(ChangeType.CREATE_SCRIPT, (long)count);

    }

    public void createLineage() throws IOException, SQLException {
        log.info("Started Lineage graph.");
        startSync(ChangeType.CREATE_LINEAGE);
        List<Script> scripts = scriptSource.getAllScripts();
        dependencyGraph.addNodes(scripts);
        List<ScriptDependency> manualDependencies = scriptSource.getManuelLineages(scripts);
        List<ScriptDependency> dependencyList = dependencyGraph.getDependencyList();
        dependencyList.addAll(manualDependencies);
        scriptRepo.insertDependencyList(dependencyList);
        endSyncSuccess(ChangeType.CREATE_LINEAGE, (long)dependencyList.size());
    }

    public void startSync(ChangeType changeType) throws SQLException {
        scriptRepo.insertChangeSync(changeType, Status.IN_PROGRESS, changeType.toString() + " started.");
    }

    public void endSyncError(ChangeType changeType, String message) throws SQLException {
        scriptRepo.updateChangeSync(changeType, Status.ERROR, message, null);
    }

    public void endSyncSuccess(ChangeType changeType, Long changeCount) throws SQLException {
        scriptRepo.updateChangeSync(changeType, Status.SUCCESS, "Successfully completed " + changeType.toString() , changeCount);
    }


}

