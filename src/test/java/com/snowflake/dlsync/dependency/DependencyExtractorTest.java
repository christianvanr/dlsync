package com.snowflake.dlsync.dependency;

import com.snowflake.dlsync.ScriptFactory;
import com.snowflake.dlsync.models.Script;
import com.snowflake.dlsync.models.ScriptObjectType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class DependencyExtractorTest {

    private DependencyExtractor dependencyExtractor;

    @BeforeEach
    void setUp() {
        dependencyExtractor = new DependencyExtractor();
    }

    @AfterEach
    void tearDown() {
    }

    List<Script> mockScripts(String name, String schema, String... contents) {
        List<Script> scripts = new ArrayList<>();
        for(int i = 0; i < contents.length; i++) {
            Script script = ScriptFactory.getStateScript("TEST_DB", schema, ScriptObjectType.VIEWS, name + i, contents[i]);
            scripts.add(script);
        }
        return scripts;

    }

    List<Script> mockScripts() {
        String[] contents = {"CREATE OR REPLACE MOCK1 AS SELECT * FROM TABLE1;", "CREATE OR REPLACE TEST_SCHEMA.MOCK2 AS SELECT * FROM MOCK1;", "CREATE OR REPLACE TEST_DB.TEST_SCHEMA.MOCK3 AS SELECT * FROM MOCK1 UNION SELECT * FROM MOCK2;"};
        List<Script> mock = mockScripts("MOCK", "TEST_SCHEMA", contents);
        mock.addAll(mockNotDependency());
        return mock;
    }

    List<Script> mockNotDependency() {
        String[] contents = {"CREATE OR REPLACE NOT_DEPENDENCY1 AS SELECT * FROM TABLE1;", "CREATE OR REPLACE TEST_SCHEMA.NOT_DEPENDENCY2 AS SELECT * FROM MOCK1;", "CREATE OR REPLACE TEST_DB.TEST_SCHEMA.NOT_DEPENDENCY3 AS SELECT * FROM MOCK1 UNION SELECT * FROM MOCK2;"};
        return mockScripts("NOT_DEPENDENCY", "TEST_SCHEMA", contents);
    }

    Script mockViewDependency(String name, String schema) {
        String content = "CREATE OR REPLACE VIEW " + name + " AS SELECT * FROM  TABLE1;";
        return ScriptFactory.getStateScript("TEST_DB", schema, ScriptObjectType.VIEWS, name, content);
    }

    Script mockTableDependency(String name, String schema) {
        String content = "CREATE OR REPLACE TABLE " + name + "(ID VARCHAR, COL1 NUMBER)";
        return ScriptFactory.getMigrationScript("TEST_DB", schema, ScriptObjectType.TABLES, name, content);
    }

    Script mockUdfDependency(String name, String schema) {
        String content = "CREATE OR REPLACE FUNCTION " + name + "(ARG1 VARCHAR)\n" +
                "RETURNS VARCHAR\n" +
                "LANGUAGE JAVASCRIPT\n" +
                "AS\n" +
                "$$\n" +
                " return ARG1.toUpperCase();\n" +
                "$$;";
        return ScriptFactory.getStateScript("TEST_DB", schema, ScriptObjectType.FUNCTIONS, name, content);
    }

    @Test
    void extractScriptDependenciesTestFrom() {
        String content = "CREATE OR REPLACE VIEW VIEW1 AS SELECT * FROM  DEPENDENCY join test_schema2.not_dependency1;";
        Script script = ScriptFactory.getStateScript("TEST_DB", "TEST_SCHEMA", ScriptObjectType.VIEWS, "VIEW1", content);
        Script dependency = mockViewDependency("DEPENDENCY", "TEST_SCHEMA");
        List<Script> changedScript = mockScripts();
        changedScript.add(script);
        changedScript.add(dependency);
        dependencyExtractor.addScripts(changedScript);
        Set<Script> expected = Set.of(
                dependency
        );
        Set<Script> actual = dependencyExtractor.extractScriptDependencies(script);
        assertEquals(expected, actual, "Dependency extractor failed:");
    }

    @Test
    void extractScriptDependenciesTestJoin() {
        String content = "CREATE OR REPLACE VIEW VIEW1 AS SELECT * FROM  DEPENDENCY JOIN test_schema.JOIN_DEPENDENCY;";
        Script script = ScriptFactory.getStateScript("TEST_DB", "TEST_SCHEMA", ScriptObjectType.VIEWS, "VIEW1", content);

        Script dependency1 = mockViewDependency("DEPENDENCY", "TEST_SCHEMA");
        Script dependency2 = mockViewDependency("JOIN_DEPENDENCY", "TEST_SCHEMA");

        List<Script> changedScript = mockScripts();
        changedScript.add(script);
        changedScript.add(dependency1);
        changedScript.add(dependency2);
        dependencyExtractor.addScripts(changedScript);
        Set<Script> expected = Set.of(
                dependency2,
                dependency1
        );
        Set<Script> actual = dependencyExtractor.extractScriptDependencies(script);
        assertEquals(expected, actual, "Dependency extractor failed:");
    }

    @Test
    void extractScriptDependenciesTestWithQuotedObjects() {
        String content = "CREATE OR REPLACE VIEW VIEW1 AS SELECT * FROM  \"TEST_SCHEMA2\".\"DEPENDENCY\" join \"TEST_SCHEMA2\".\"NOT_DEPENDENCY1\"";
        Script script = ScriptFactory.getStateScript("TEST_DB", "TEST_SCHEMA", ScriptObjectType.VIEWS, "VIEW1", content);

        Script dependency1 = mockViewDependency("DEPENDENCY", "TEST_SCHEMA2");

        List<Script> changedScript = mockScripts();
        changedScript.add(script);
        changedScript.add(dependency1);
        dependencyExtractor.addScripts(changedScript);
        Set<Script> expected = Set.of(
                dependency1
        );
        Set<Script> actual = dependencyExtractor.extractScriptDependencies(script);
        assertEquals(expected, actual, "Dependency extractor failed:");
    }

    @Test
    void extractScriptDependenciesTestWithComments() throws IOException {
        String content = "CREATE OR REPLACE VIEW VIEW1 COMMENT='SOME COMMENTS' AS SELECT * FROM -- NOT_DEPENDENCY1\n" +
                "DEPENDENCY1 T1\n" +
                "JOIN  \n" +
                "// NOT_DEPENDENCY2 TC2 ON T1.ID = TC2.ID\n" +
                "DEPENDENCY2 TC3 ON T1.ID = TC3.ID\n" +
                "JOIN /*\n" +
                "ADDITIONAL COMMENTS HERE\n" +
                "SELECT * FROM NOT_DEPENDENCY2;\n" +
                "*/\n" +
                "DEPENDENCY3 T4 ON T4.ID = T1.ID;";
        Script script = ScriptFactory.getStateScript("TEST_DB", "TEST_SCHEMA", ScriptObjectType.VIEWS, "VIEW1", content);

        Script dependency1 = mockViewDependency("DEPENDENCY1", "TEST_SCHEMA");
        Script dependency2 = mockViewDependency("DEPENDENCY2", "TEST_SCHEMA");
        Script dependency3 = mockViewDependency("DEPENDENCY3", "TEST_SCHEMA");

        Script notDependency1 = mockViewDependency("NOT_DEPENDENCY1", "TEST_SCHEMA");
        Script notDependency2 = mockViewDependency("NOT_DEPENDENCY2", "TEST_SCHEMA");


        List<Script> changedScript = mockScripts();
        changedScript.add(script);
        changedScript.add(dependency1);
        changedScript.add(dependency2);
        changedScript.add(dependency3);
        changedScript.add(notDependency1);
        changedScript.add(notDependency2);

        dependencyExtractor.addScripts(changedScript);
        Set<Script> expected = Set.of(
                dependency2,
                dependency1,
                dependency3
        );
        Set<Script> actual = dependencyExtractor.extractScriptDependencies(script);
        assertEquals(expected, actual, "Dependency extractor failed:");

    }

    @Test
    void extractScriptDependenciesTestWithCTE() {
        String content = "CREATE OR REPLACE VIEW VIEW1 AS wITH CTE1 AS (\n" +
                "        SELECT * FROM DEPENDENCY1\n" +
                "        ),\n" +
                "       CTE2 AS(\n" +
                "             SELECT B.*, JLS.VALUE::STRING AS JOURNAL_LINE\n" +
                "             FROM DEPENDENCY2 B\n" +
                "                      JOIN JOIN_DEPENDENCY2\n" +
                "         ),CTE3 AS (\n" +
                "             SELECT * FROM CTE1 WHERE ID IN (SELECT ID FROM CTE2) \n" +
                "         ), CTE4 AS (\n" +
                "             SELECT * FROM DEPENDENCY3 D1 JOIN CTE3 C3 ON D1.ID=C3.ID\n" +
                "         ), CTE5 AS (SELECT * FROM CTE4 JOIN JOIN_DEPENDENCY3)\n" +
                "    SELECT  * FROM CTE1 C1 JOIN CTE5 CT5 ON CT1.ID=CT5.ID JOIN JOIN_DEPENDENCY4 ON CT1.ID=CT5.ID;";

        Script script = ScriptFactory.getStateScript("TEST_DB", "TEST_SCHEMA", ScriptObjectType.VIEWS, "VIEW1", content);

        Script dependency1 = mockViewDependency("DEPENDENCY1", "TEST_SCHEMA");
        Script dependency2 = mockViewDependency("DEPENDENCY2", "TEST_SCHEMA");
        Script dependency3 = mockViewDependency("DEPENDENCY3", "TEST_SCHEMA");
        Script joinDependency2 = mockViewDependency("JOIN_DEPENDENCY2", "TEST_SCHEMA");
        Script joinDependency3 = mockViewDependency("JOIN_DEPENDENCY3", "TEST_SCHEMA");
        Script joinDependency4 = mockViewDependency("JOIN_DEPENDENCY4", "TEST_SCHEMA");

        List<Script> changedScript = mockScripts();
        changedScript.add(script);
        changedScript.add(dependency1);
        changedScript.add(dependency2);
        changedScript.add(dependency3);
        changedScript.add(joinDependency2);
        changedScript.add(joinDependency3);
        changedScript.add(joinDependency4);
        dependencyExtractor.addScripts(changedScript);
        Set<Script> expected = Set.of(
                dependency1,
                dependency2,
                dependency3,
                joinDependency2,
                joinDependency3,
                joinDependency4
        );
        Set<Script> actual = dependencyExtractor.extractScriptDependencies(script);
        assertEquals(expected, actual, "Dependency extractor failed:");

    }

    @Test
    void extractScriptDependenciesTestWithQuoted() {
        String content = "CREATE OR REPLACE VIEW VIEW1 AS SELECT 'SELECT * FROM NOT_DEPENDENCY1', * FROM  DEPENDENCY1";
        Script script = ScriptFactory.getStateScript("TEST_DB", "TEST_SCHEMA", ScriptObjectType.VIEWS, "VIEW1", content);
        Script dependency1 = mockViewDependency("DEPENDENCY1", "TEST_SCHEMA");
        List<Script> changedScript = mockScripts();
        changedScript.add(script);
        changedScript.add(dependency1);
        dependencyExtractor.addScripts(changedScript);
        Set<Script> expected = Set.of(
                dependency1
        );
        Set<Script> actual = dependencyExtractor.extractScriptDependencies(script);
        //TODO:Fix this
//        assertEquals(expected, actual, "Dependency extractor failed:");
    }

    @Test
    void extractScriptDependenciesTestWithAliases() {
        String content = "CREATE OR REPLACE VIEW VIEW1 AS SELECT  * FROM  DEPENDENCY1 as dp1, DEPENDENCY2 dp2, DEPENDENCY3 AS dp3 where dp1.id=dp2.id and dp2.id=dp3.id;";
        Script script = ScriptFactory.getStateScript("TEST_DB", "TEST_SCHEMA", ScriptObjectType.VIEWS, "VIEW1", content);
        Script dependency1 = mockViewDependency("DEPENDENCY1", "TEST_SCHEMA");
        Script dependency2 = mockViewDependency("DEPENDENCY2", "TEST_SCHEMA");
        Script dependency3 = mockViewDependency("DEPENDENCY3", "TEST_SCHEMA");

        List<Script> changedScript = mockScripts();
        changedScript.add(script);
        changedScript.add(dependency1);
        changedScript.add(dependency2);
        changedScript.add(dependency3);
        dependencyExtractor.addScripts(changedScript);
        Set<Script> expected = Set.of(
                dependency2,
                dependency1,
                dependency3
        );
        Set<Script> actual = dependencyExtractor.extractScriptDependencies(script);
        assertEquals(expected, actual, "Dependency extractor failed:");

    }
    @Test
    void extractScriptDependenciesWithSameObjectName() {
        String content = "CREATE OR REPLACE VIEW TEST_SCHEMA.VIEW1 AS SELECT * FROM  TEST_SCHEMA2.VIEW1 join test_schema2.not_dependency1;";
        Script script = ScriptFactory.getStateScript("TEST_DB", "TEST_SCHEMA", ScriptObjectType.VIEWS, "VIEW1", content);
        Script dependency = mockViewDependency("VIEW1", "TEST_SCHEMA2");
        List<Script> changedScript = mockScripts();
        changedScript.add(script);
        changedScript.add(dependency);
        dependencyExtractor.addScripts(changedScript);
        Set<Script> expected = Set.of(
                dependency
        );
        Set<Script> actual = dependencyExtractor.extractScriptDependencies(script);
        assertEquals(expected, actual, "Dependency extractor failed:");
    }

    @Test
    void extractScriptDependenciesWithUdf() {
        String content = "CREATE OR REPLACE VIEW TEST_SCHEMA.VIEW1 AS SELECT COL1, COL2, TEST_SCHEMA1.UDF1(COL3) AS COL4 FROM  TEST_SCHEMA2.VIEW1 join test_schema2.not_dependency1;";
        Script script = ScriptFactory.getStateScript("TEST_DB", "TEST_SCHEMA", ScriptObjectType.VIEWS, "VIEW1", content);
        Script dependency1 = mockViewDependency("VIEW1", "TEST_SCHEMA2");
        Script dependency2 = mockUdfDependency("UDF1", "TEST_SCHEMA1");
        List<Script> changedScript = mockScripts();
        changedScript.add(script);
        changedScript.add(dependency1);
        changedScript.add(dependency2);
        dependencyExtractor.addScripts(changedScript);
        Set<Script> expected = Set.of(
                dependency1,
                dependency2
        );
        Set<Script> actual = dependencyExtractor.extractScriptDependencies(script);
        assertEquals(expected, actual, "Dependency extractor failed:");
    }

    @Test
    void extractScriptDependenciesTableWithViewDependency() {
        String content =
        "---version: 0, author: dlsync\n" +
        "CREATE OR REPLACE TABLE TEST_SCHEMA.TABLE1 AS SELECT * FROM TEST_SCHEMA.VIEW1;\n";

        Script script = ScriptFactory.getStateScript("TEST_DB", "TEST_SCHEMA", ScriptObjectType.TABLES, "TABLE1", content);
        Script dependency1 = mockViewDependency("VIEW1", "TEST_SCHEMA");
        List<Script> changedScript = mockScripts();
        changedScript.add(script);
        changedScript.add(dependency1);
        dependencyExtractor.addScripts(changedScript);
        Set<Script> expected = Set.of(
                dependency1
        );
        Set<Script> actual = dependencyExtractor.extractScriptDependencies(script);
        assertEquals(expected, actual, "Dependency extractor failed:");
    }

    @Test
    void extractScriptDependenciesWithStringNames() {
        String content =
                "---version: 1, author: dlsync\n" +
                "INSERT INTO TEST_SCHEMA.TABLE1 values(1, 'not_dependency1');\n";

        Script script = ScriptFactory.getMigrationScript("TEST_DB", "TEST_SCHEMA", ScriptObjectType.TABLES, "TABLE1", content, 1L, "dlsync", "", "");
        Script dependency1 = mockTableDependency("TABLE1", "TEST_SCHEMA");
        Script not_dependency1 = mockViewDependency("NOT_DEPENDENCY1", "TEST_SCHEMA");
        List<Script> changedScript = mockScripts();
        changedScript.add(script);
        changedScript.add(dependency1);
        changedScript.add(not_dependency1);
        dependencyExtractor.addScripts(changedScript);
        Set<Script> expected = Set.of(
                dependency1
        );
        Set<Script> actual = dependencyExtractor.extractScriptDependencies(script);
        assertEquals(expected, actual, "Dependency extractor failed:");
    }

    @Test
    void extractScriptDependenciesIdentifierAfterEquals() {
        String content = "CREATE OR REPLACE VIEW TEST_SCHEMA.VIEW1 AS SELECT COL1, COL2=FUNC1(COL3) FROM  TEST_SCHEMA2.VIEW1 join test_schema2.not_dependency1;";
        Script script = ScriptFactory.getStateScript("TEST_DB", "TEST_SCHEMA", ScriptObjectType.VIEWS, "VIEW1", content);
        Script dependency1 = mockViewDependency("VIEW1", "TEST_SCHEMA2");
        Script dependency2 = mockUdfDependency("FUNC1", "TEST_SCHEMA");
        List<Script> changedScript = mockScripts();
        changedScript.add(script);
        changedScript.add(dependency1);
        changedScript.add(dependency2);
        dependencyExtractor.addScripts(changedScript);
        Set<Script> expected = Set.of(
                dependency1,
                dependency2
        );
        Set<Script> actual = dependencyExtractor.extractScriptDependencies(script);
        assertEquals(expected, actual, "Dependency extractor failed:");
    }

    // TODO: Fix this failing test
    void extractScriptDependenciesWithSameName() {
        String content = "CREATE OR REPLACE VIEW TEST_SCHEMA.VIEW1 AS SELECT COL1, COL2 FROM  TEST_SCHEMA2.VIEW1 join test_schema2.not_dependency1;";
        Script script = ScriptFactory.getStateScript("TEST_DB", "TEST_SCHEMA", ScriptObjectType.VIEWS, "VIEW1", content);
        Script dependency1 = mockViewDependency("VIEW1", "TEST_SCHEMA2");
        Script notDependency = mockUdfDependency("VIEW1", "TEST_SCHEMA2");
        List<Script> changedScript = mockScripts();
        changedScript.add(script);
        changedScript.add(dependency1);
        changedScript.add(notDependency);
        dependencyExtractor.addScripts(changedScript);
        Set<Script> expected = Set.of(
                dependency1
        );
        Set<Script> actual = dependencyExtractor.extractScriptDependencies(script);
        assertEquals(expected, actual, "Dependency extractor failed:");
    }


}