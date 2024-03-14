import io.github.cdimascio.dotenv.Dotenv;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class MainTest {
    private Connection connection;
    private StringWriter consoleOutput;
    private PrintStream originalSystemOut;

    @BeforeEach
    public void setUp() throws SQLException, IOException {
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

    @AfterEach
    public void tearDown() throws SQLException {
        connection.close();
        System.setOut(originalSystemOut);
    }

    @Test
    public void testDataInsertion() {
        Main.main(null);
    }

    @Test
    public void testExecuteScript() throws SQLException {
        Main.executeScript(connection, "src/main/resources/sql/init_tables.sql");
    }

    @Test
    public void testLoadProperties() throws IOException {
        Properties properties = Main.loadProperties("src/test/resources/test_configuration.properties");

        assertNotNull(properties);
        assertEquals("jdbc:postgresql://localhost:5432/mydatabase", properties.getProperty("POSTGRES_URL"));
        assertEquals("myuser", properties.getProperty("POSTGRES_USER"));
        assertEquals("secret", properties.getProperty("POSTGRES_PASSWORD"));
        assertEquals("src/main/resources/sql/", properties.getProperty("SQL_FILE_PATH"));

    }

    @Test
    public void testTb2_2Data() throws SQLException {
        Main.executeScripts(connection, "src/main/resources/sql/", "code2.sql");
        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_tb2_2");
             ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(8, rowCount);
        }
    }

    @Test
    public void testTb2_1Data() throws SQLException {
        Main.executeScripts(connection, "src/main/resources/sql/", "code1.sql");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_tb2_1");
             ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(70, rowCount);
        }
    }

    @Test
    public void testtab_t_5_c_pData() throws SQLException {
        Main.executeScripts(connection, "src/main/resources/sql/", "code4.sql");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_t_5_c_p");
             ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(1, rowCount);
        }

    }

    @Test
    public void testtab_t_5_c_sData() throws SQLException {
        Main.executeScripts(connection, "src/main/resources/sql/", "code5.sql");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_t_5_c_s");
             ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(1, rowCount);
        }

    }


    @Test
    public void testtab_t_a_l_p_bData() throws SQLException {
        Main.executeScripts(connection, "src/main/resources/sql/", "code7.sql");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_t_a_l_p_b");
             ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(30, rowCount);
        }

    }

    @Test
    public void testtab_t_a_l_s_bData() throws SQLException {
        Main.executeScripts(connection, "src/main/resources/sql/", "code8.sql");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_t_a_l_s_b");
             ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(30, rowCount);
        }

    }
}
