import io.github.cdimascio.dotenv.Dotenv;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
public class CreateQueryTest {
    private static Connection connection;
    private static StringWriter consoleOutput;

    @BeforeAll
    public static void setUp() throws SQLException, IOException {
        Dotenv dotenv = Dotenv.configure().load();

        String testDbUrl = dotenv.get("POSTGRES_URL");
        String testDbUser = dotenv.get("POSTGRES_USER");
        String testDbPassword = dotenv.get("POSTGRES_PASSWORD");

        connection = DriverManager.getConnection(testDbUrl, testDbUser, testDbPassword);
        consoleOutput = new StringWriter();
        System.out.println("Setting up the test database...");

        Main.executeScript(connection, "src/main/resources/sql/init_tables.sql");

        CopyManager copyManager = new CopyManager((BaseConnection) connection);
        insertTestData(copyManager);

        System.out.println("Test database setup completed.");
    }

    private static void insertTestData(CopyManager copyManager) throws SQLException, IOException {
        Main.insertData("src/main/resources/big-csv/tab_c_gt.csv", "demo.tab_c_gt", copyManager);
        Main.insertData("src/main/resources/big-csv/tab_oab.csv", "demo.tab_oab", copyManager);
        Main.insertData("src/main/resources/big-csv/tab_sbfa.csv", "demo.tab_sbfa", copyManager);
        Main.insertData("src/main/resources/big-csv/tab_t_5_c_c_1.csv", "demo.tab_t_5_c_c_1", copyManager);
    }

    @AfterAll
    public static void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    public void testCode1() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code1.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        List<String> columnNames = Arrays.asList("col_s_1", "col_s_c", "col_d_c", "col_c_cv", "col_s_r", "col_c_r");

        assertRowCountAndColumnCount("tab_tb2_1", 6, 70, columnNames);
    }

    @Test
    public void testCode2() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code2.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        List<String> columnNames = Arrays.asList("col_s_1", "col_t_m", "col_m_t_d", "col_s_c", "col_d_c", "col_c_cv", "col_s_r", "col_c_r");

        assertRowCountAndColumnCount("tab_tb2_2", 8, 8, columnNames);
    }

    @Test
    public void testCode4() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code4.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        List<String> columnNames = Arrays.asList("col_o_i", "col_s_c_c");

        assertRowCountAndColumnCount("tab_t_5_c_p", 2, 1, columnNames);
    }

    @Test
    public void testCode5() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code5.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        List<String> columnNames = Arrays.asList("col_o_i", "col_s_c_c");

        assertRowCountAndColumnCount("tab_t_5_c_s", 2, 1, columnNames);
    }


    @Test
    public void testCode7() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code7.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        List<String> columnNames = Arrays.asList("col_s_1", "col_o_i", "col_s_t_d", "col_s_c", "col_s_r");


        assertRowCountAndColumnCount("tab_t_a_l_p_b", 5, 31, columnNames);
    }

    @Test
    public void testCode8() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code8.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        List<String> columnNames = Arrays.asList("col_s_1", "col_o_i", "col_s_t_d", "col_s_c", "col_s_r");

        assertRowCountAndColumnCount("tab_t_a_l_s_b", 5, 31, columnNames);

    }

    private void assertRowCountAndColumnCount(String tableName, int expectedColumnCount, int expectedRowCount, List<String> expectedColumnNames) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo." + tableName);
             ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(expectedRowCount, rowCount);

            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet columns = metaData.getColumns(null, null, tableName, null)) {
                int columnCount = 0;
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    assertEquals(expectedColumnNames.get(columnCount), columnName);
                    columnCount++;
                }
                assertEquals(expectedColumnCount, columnCount);
            }
        }
    }
}
