package com.snowflake.dlsync;

import com.snowflake.dlsync.dependency.DependencyExtractor;
import com.snowflake.dlsync.dependency.DependencyGraph;
import com.snowflake.dlsync.doa.ScriptRepo;
import com.snowflake.dlsync.doa.ScriptSource;
import com.snowflake.dlsync.parser.ParameterInjector;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@Slf4j
public class ChangeMangerFactory {
    public static ChangeManager createChangeManger() throws IOException, SQLException {
        ConfigManager configManager = new ConfigManager();
        return createChangeManger(configManager);
    }

    public static ChangeManager createChangeManger(String scriptRoot, String profile) throws IOException, SQLException {
        ConfigManager configManager = new ConfigManager(scriptRoot, profile);
        return createChangeManger(configManager);
    }

    public static ChangeManager createChangeManger(ConfigManager configManager) throws IOException, SQLException {
        configManager.init();

        // Create connection
        Connection connection = createConnection(configManager.getConfig().getConnection());

        // Create dependencies
        ScriptSource scriptSource = new ScriptSource(configManager.getScriptRoot());
        ScriptRepo scriptRepo = new ScriptRepo(connection, configManager.getConfig().getConnection());
        scriptRepo.init();
        ParameterInjector parameterInjector = new ParameterInjector(configManager.getScriptParameters());
        DependencyExtractor dependencyExtractor = new DependencyExtractor();
        DependencyGraph dependencyGraph = new DependencyGraph(dependencyExtractor, configManager.getConfig());

        return new ChangeManager(configManager.getConfig(), scriptSource, scriptRepo, dependencyGraph, parameterInjector);
    }

    /**
     * Create a JDBC connection to Snowflake
     */
    private static Connection createConnection(Properties connectionProperties) throws SQLException {
        String account = connectionProperties.getProperty("account");
        if (account == null || account.isEmpty()) {
            throw new SQLException("Missing 'account' property in connection configuration");
        }

        String jdbcUrl = "jdbc:snowflake://" + account + ".snowflakecomputing.com/";
        log.debug("Creating Snowflake connection for account: {}", account);


        try {
            return DriverManager.getConnection(jdbcUrl, connectionProperties);
        } catch (SQLException e) {
            log.error("Failed to create Snowflake connection: {}", e.getMessage());
            throw new SQLException("Unable to connect to Snowflake. Please check your account and connection properties.", e);
        }
    }
}
