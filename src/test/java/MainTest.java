import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class MainTest {

    private static final String TEST_DB_URL = "jdbc:postgresql://localhost:5432/mydatabase";
    private static final String TEST_DB_USER = "myuser";
    private static final String TEST_DB_PASSWORD = "secret";

    private Connection connection;
    private StringWriter consoleOutput;
    private PrintStream originalSystemOut;

    @BeforeEach
    public void setUp() throws SQLException, IOException {
        connection = DriverManager.getConnection(TEST_DB_URL, TEST_DB_USER, TEST_DB_PASSWORD);
        consoleOutput = new StringWriter();
        originalSystemOut = System.out;
        System.out.println("Setting up the test database...");
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
        Main.executeScript(connection, "src/test/resources/sql/init_tables.sql");
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
        Main.executeScript(connection, "src/test/resources/sql/init_tables.sql");
        Main.executeScripts(connection, "src/test/resources/sql/", "code2.sql");
        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_tb2_2");
             ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(0, rowCount);
        }
    }

    @Test
    public void testTb2_1Data() throws SQLException {
        Main.executeScript(connection, "src/test/resources/sql/init_tables.sql");
        Main.executeScripts(connection, "src/test/resources/sql/", "code1.sql");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_tb2_1");
             ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(70, rowCount);
        }
    }

    @Test
    public void testtab_t_5_c_pData() throws SQLException {
        Main.executeScript(connection, "src/test/resources/sql/init_tables.sql");
        Main.executeScripts(connection, "src/test/resources/sql/", "code4.sql");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_t_5_c_p");
             ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(1, rowCount);
        }

    }

    @Test
    public void testtab_t_5_c_sData() throws SQLException {
        Main.executeScript(connection, "src/test/resources/sql/init_tables.sql");
        Main.executeScripts(connection, "src/test/resources/sql/", "code5.sql");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_t_5_c_s");
             ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(1, rowCount);
        }

    }


    @Test
    public void testtab_t_a_l_p_bData() throws SQLException {
        Main.executeScript(connection, "src/test/resources/sql/init_tables.sql");
        Main.executeScripts(connection, "src/test/resources/sql/", "code7.sql");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_t_a_l_p_b");
             ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(0, rowCount);
        }

    }

    @Test
    public void testtab_t_a_l_s_bData() throws SQLException {
        Main.executeScript(connection, "src/test/resources/sql/init_tables.sql");
        Main.executeScripts(connection, "src/test/resources/sql/", "code8.sql");

        try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM demo.tab_t_a_l_s_b");
             ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            int rowCount = resultSet.getInt(1);
            assertEquals(0, rowCount);
        }

    }
}
