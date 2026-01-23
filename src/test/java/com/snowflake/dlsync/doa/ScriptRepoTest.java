package com.snowflake.dlsync.doa;

import com.snowflake.dlsync.models.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ScriptRepoTest {

    @Mock
    private Connection mockConnection;

    @Mock
    private Statement mockStatement;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private ResultSet mockResultSet;

    private ScriptRepo scriptRepo;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);

        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockConnection.getAutoCommit()).thenReturn(true);

        when(mockStatement.executeQuery(anyString()))
            .thenReturn(mockResultSet);

        when(mockResultSet.next())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);

        when(mockResultSet.getString(1)).thenReturn("TEST_DB");
        when(mockResultSet.getString(2)).thenReturn("PUBLIC");
        when(mockResultSet.getLong(1)).thenReturn(0L);

        when(mockStatement.executeUpdate(anyString())).thenReturn(1);
        when(mockPreparedStatement.executeUpdate()).thenReturn(1);

        scriptRepo = createScriptRepoWithMockedConnection();

        scriptRepo.insertChangeSync(ChangeType.DEPLOY, Status.IN_PROGRESS, "Test deployment");
    }

    @Test
    void testCreateScriptObjectOnlyHashesFalseExecutesScript() throws SQLException {
        SchemaScript script = new SchemaScript(
            "test/MY_TABLE.sql",
            "TEST_DB",
            "TEST_SCHEMA",
            "MY_TABLE",
            ScriptObjectType.TABLES,
            "CREATE TABLE MY_TABLE (id INT, name VARCHAR);"
        );
        script.setHash("abc123def456");

        when(mockStatement.execute(anyString())).thenReturn(false);
        when(mockPreparedStatement.executeUpdate()).thenReturn(1);

        scriptRepo.createScriptObject(script, false);

        ArgumentCaptor<String> executeCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockStatement, times(1)).execute(executeCaptor.capture());
        assertEquals("CREATE TABLE MY_TABLE (id INT, name VARCHAR);", executeCaptor.getValue(),
            "Script content should be executed when onlyHashes=false");

        verify(mockConnection, atLeastOnce()).setAutoCommit(false);
        verify(mockConnection, atLeastOnce()).setAutoCommit(true);
        verify(mockConnection, atLeastOnce()).commit();

        verify(mockPreparedStatement, atLeastOnce()).executeUpdate();
    }

    @Test
    void testCreateScriptObjectOnlyHashesTrueSkipsScriptExecution() throws SQLException {
        SchemaScript script = new SchemaScript(
            "test/MY_VIEW.sql",
            "TEST_DB",
            "TEST_SCHEMA",
            "MY_VIEW",
            ScriptObjectType.VIEWS,
            "CREATE VIEW MY_VIEW AS SELECT * FROM MY_TABLE;"
        );
        script.setHash("xyz789uvw123");

        when(mockPreparedStatement.executeUpdate()).thenReturn(1);

        scriptRepo.createScriptObject(script, true);

        verify(mockStatement, never()).execute(anyString());

        verify(mockConnection, atLeastOnce()).setAutoCommit(false);
        verify(mockConnection, atLeastOnce()).setAutoCommit(true);
        verify(mockConnection, atLeastOnce()).commit();

        verify(mockPreparedStatement, atLeastOnce()).executeUpdate();
    }

    @Test
    void testOnlyHashesParameterControls() throws SQLException {
        SchemaScript scriptWithExecution = new SchemaScript(
            "test/TABLE1.sql",
            "TEST_DB",
            "TEST_SCHEMA",
            "TABLE1",
            ScriptObjectType.TABLES,
            "CREATE TABLE TABLE1 (id INT);"
        );
        scriptWithExecution.setHash("hash001");

        SchemaScript scriptHashOnly = new SchemaScript(
            "test/TABLE2.sql",
            "TEST_DB",
            "TEST_SCHEMA",
            "TABLE2",
            ScriptObjectType.TABLES,
            "CREATE TABLE TABLE2 (id INT);"
        );
        scriptHashOnly.setHash("hash002");

        when(mockStatement.execute(anyString())).thenReturn(false);
        when(mockPreparedStatement.executeUpdate()).thenReturn(1);

        scriptRepo.createScriptObject(scriptWithExecution, false);
        scriptRepo.createScriptObject(scriptHashOnly, true);

        verify(mockStatement, times(1)).execute(anyString());

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockStatement, times(1)).execute(captor.capture());
        assertEquals("CREATE TABLE TABLE1 (id INT);", captor.getValue(),
            "Only the first script (onlyHashes=false) should be executed");
    }

    @Test
    void testBothModeUpdateHashes() throws SQLException {
        MigrationScript migrationScript = new MigrationScript(
            new SchemaScript("test/MY_TABLE.sql", "TEST_DB", "TEST_SCHEMA", "MY_TABLE", ScriptObjectType.TABLES, ""),
            "CREATE TABLE MY_TABLE (id INT);",
            0L,
            "admin",
            "DROP TABLE MY_TABLE;",
            "SELECT COUNT(*) FROM MY_TABLE;"
        );
        migrationScript.setHash("migration_hash");

        when(mockStatement.execute(anyString())).thenReturn(false);
        when(mockPreparedStatement.executeUpdate()).thenReturn(1);

        scriptRepo.createScriptObject(migrationScript, true);

        verify(mockStatement, never()).execute(anyString());

        verify(mockConnection, atLeastOnce()).prepareStatement(anyString());
        verify(mockPreparedStatement, atLeastOnce()).executeUpdate();

        verify(mockConnection, atLeastOnce()).commit();
    }

    private ScriptRepo createScriptRepoWithMockedConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty("account", "test_account");
        return new ScriptRepo(mockConnection, props);
    }
}



