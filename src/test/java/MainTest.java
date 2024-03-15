import io.github.cdimascio.dotenv.Dotenv;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

public class MainTest {
    private static Connection connection;
    private static StringWriter consoleOutput;
    private static PrintStream originalSystemOut;

    @BeforeAll
    public static void setUp() throws SQLException, IOException {
        Dotenv dotenv = Dotenv.configure().load();

        String testDbUrl = dotenv.get("POSTGRES_URL");
        String testDbUser = dotenv.get("POSTGRES_USER");
        String testDbPassword = dotenv.get("POSTGRES_PASSWORD");

        connection = DriverManager.getConnection(testDbUrl, testDbUser, testDbPassword);
        consoleOutput = new StringWriter();
        originalSystemOut = System.out;
        System.out.println("Setting up the test database...");

        Main.executeScript(connection, "src/main/resources/sql/init_tables.sql");

        CopyManager copyManager = new CopyManager((BaseConnection) connection);
        Main.insertData("src/main/resources/csv/tab_c_gt.csv", "demo.tab_c_gt", copyManager);
        Main.insertData("src/main/resources/csv/tab_oab.csv", "demo.tab_oab", copyManager);
        Main.insertData("src/main/resources/csv/tab_sbfa.csv", "demo.tab_sbfa", copyManager);
        Main.insertData("src/main/resources/csv/tab_t_5_c_c_1.csv", "demo.tab_t_5_c_c_1", copyManager);

        System.out.println("Test database setup completed.");
    }

    @AfterAll
    public static void tearDown() throws SQLException {
        connection.close();
        System.setOut(originalSystemOut);
    }

    @Test
    public void testCode2() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code2.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime)/1000.0;
        System.out.println("Duration: " + duration + "s");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_tb2_2"); ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(8, rowCount);
        }

        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet columns = metaData.getColumns(null, null, "tab_tb2_2", null)) {
            int columnCount = 0;
            while (columns.next()) {
                columnCount++;
            }
            assertEquals(8, columnCount);
        }
    }

    @Test
    public void testCode1() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code1.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime)/1000.0;
        System.out.println("Duration: " + duration + "s");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_tb2_1"); ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(70, rowCount);
        }

        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet columns = metaData.getColumns(null, null, "tab_tb2_1", null)) {
            int columnCount = 0;
            while (columns.next()) {
                columnCount++;
            }
            assertEquals(6, columnCount);
        }
    }

    @Test
    public void testCode4() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code4.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime)/1000.0;
        System.out.println("Duration: " + duration + "s");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_t_5_c_p"); ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(1, rowCount);
        }

        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet columns = metaData.getColumns(null, null, "tab_t_5_c_p", null)) {
            int columnCount = 0;
            while (columns.next()) {
                columnCount++;
            }
            assertEquals(2, columnCount);
        }
    }

    @Test
    public void testCode5() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code5.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime)/1000.0;
        System.out.println("Duration: " + duration + "s");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_t_5_c_s"); ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(1, rowCount);
        }

        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet columns = metaData.getColumns(null, null, "tab_t_5_c_s", null)) {
            int columnCount = 0;
            while (columns.next()) {
                columnCount++;
            }
            assertEquals(2, columnCount);
        }
    }


    @Test
    public void testCode7() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code7.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime)/1000.0;
        System.out.println("Duration: " + duration + "s");
        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_t_a_l_p_b"); ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(30, rowCount);
        }

        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet columns = metaData.getColumns(null, null, "tab_t_a_l_p_b", null)) {
            int columnCount = 0;
            while (columns.next()) {
                columnCount++;
            }
            assertEquals(5, columnCount);
        }
    }

    @Test
    public void testCode8() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code8.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime)/1000.0;
        System.out.println("Duration: " + duration + "s");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_t_a_l_s_b"); ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(30, rowCount);
        }

        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet columns = metaData.getColumns(null, null, "tab_t_a_l_s_b", null)) {
            int columnCount = 0;
            while (columns.next()) {
                columnCount++;
            }
            assertEquals(5, columnCount);
        }
    }
}
