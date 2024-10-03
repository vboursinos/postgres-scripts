import io.github.cdimascio.dotenv.Dotenv;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
public class MainTest {
    private static Connection connection;
    private static StringWriter consoleOutput;
    private static String resultFilePath = "src/main/resources/results/";

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

        File resultFolder = new File(resultFilePath);
        if (!resultFolder.exists()) {
            boolean success = resultFolder.mkdirs();
            if (!success) {
                throw new IOException("Failed to create result folder: " + resultFilePath);
            }
        }
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
        deleteFolder(new File(resultFilePath));
    }

    @Test
    public void testCode1() throws SQLException, IOException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code1.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        File resultFile = new File("src/main/resources/results/file1.txt");
        File correctResultFile = new File("src/main/resources/correct-results/file1.txt");
        System.out.println("Duration: " + duration + "s");
        List<String> columnNames = Arrays.asList("col_s_1", "col_s_c", "col_d_c", "col_c_cv", "col_s_r", "col_c_r");

        assertRowCountAndColumnCount("tab_tb2_1", 6, 70, columnNames);
        assertCompareSqlResults(resultFile, correctResultFile, "tab_tb2_1");
    }

    @Test
    public void testCode2() throws SQLException, IOException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code2.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        File resultFile = new File("src/main/resources/results/file2.txt");
        File correctResultFile = new File("src/main/resources/correct-results/file2.txt");
        System.out.println("Duration: " + duration + "s");
        List<String> columnNames = Arrays.asList("col_s_1", "col_t_m", "col_m_t_d", "col_s_c", "col_d_c", "col_c_cv", "col_s_r", "col_c_r");

        assertRowCountAndColumnCount("tab_tb2_2", 8, 8, columnNames);
        assertCompareSqlResults(resultFile, correctResultFile, "tab_tb2_2");
    }

    @Test
    public void testCode4() throws SQLException, IOException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code4.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        File resultFile = new File("src/main/resources/results/file4.txt");
        File correctResultFile = new File("src/main/resources/correct-results/file4.txt");
        System.out.println("Duration: " + duration + "s");
        List<String> columnNames = Arrays.asList("col_o_i", "col_s_c_c");

        assertRowCountAndColumnCount("tab_t_5_c_p", 2, 1, columnNames);
        assertCompareSqlResults(resultFile, correctResultFile, "tab_t_5_c_p");
    }

    @Test
    public void testCode5() throws SQLException, IOException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code5.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        File resultFile = new File("src/main/resources/results/file5.txt");
        File correctResultFile = new File("src/main/resources/correct-results/file5.txt");
        System.out.println("Duration: " + duration + "s");
        List<String> columnNames = Arrays.asList("col_o_i", "col_s_c_c");

        assertRowCountAndColumnCount("tab_t_5_c_s", 2, 1, columnNames);
        assertCompareSqlResults(resultFile, correctResultFile, "tab_t_5_c_s");
    }


    @Test
    public void testCode7() throws SQLException, IOException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code7.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        File resultFile = new File("src/main/resources/results/file7.txt");
        File correctResultFile = new File("src/main/resources/correct-results/file7.txt");
        System.out.println("Duration: " + duration + "s");
        List<String> columnNames = Arrays.asList("col_s_1", "col_o_i", "col_s_t_d", "col_s_c", "col_s_r");

        assertRowCountAndColumnCount("tab_t_a_l_p_b", 5, 31, columnNames);
        assertCompareSqlResults(resultFile, correctResultFile, "tab_t_a_l_p_b");
    }

    @Test
    public void testCode8() throws SQLException, IOException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/sql/", "code8.sql");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        File resultFile = new File("src/main/resources/results/file8.txt");
        File correctResultFile = new File("src/main/resources/correct-results/file8.txt");
        System.out.println("Duration: " + duration + "s");
        List<String> columnNames = Arrays.asList("col_s_1", "col_o_i", "col_s_t_d", "col_s_c", "col_s_r");

        assertRowCountAndColumnCount("tab_t_a_l_s_b", 5, 31, columnNames);
        assertCompareSqlResults(resultFile, correctResultFile, "tab_t_a_l_s_b");
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

    private void assertCompareSqlResults(File resultFile, File correctResultFile, String tableName) throws IOException {
        String sql = "SELECT * FROM demo." + tableName+";";
        writeQueryResults(sql,resultFile);

        Set<String> resultLines = new HashSet<>(Files.readAllLines(resultFile.toPath(), StandardCharsets.UTF_8));
        Set<String> correctResultLines = new HashSet<>(Files.readAllLines(correctResultFile.toPath(), StandardCharsets.UTF_8));

        assertEquals(correctResultLines, resultLines, "The files content are not the same");
        System.out.println("Assertion passed: The files content are the same.");
    }

    private void writeQueryResults(String sql, File outputFilePath) {
        try (Statement statement = connection.createStatement();
             FileWriter writer = new FileWriter(outputFilePath)) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (resultSet.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        String result = resultSet.getString(i);
                        if (result == null) {
                            result = "NULL"; // Write empty string if result is null
                        }
                        writer.write(result);
                        if (i < columnCount) {
                            writer.write(", "); // Separate columns with a comma
                        }
                    }
                    writer.write("\n"); // Start a new line for each row
                }
            } catch (SQLException e) {
                // Handle any SQL exception during query execution
                e.printStackTrace();
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void deleteFolder(File folder) {
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteFolder(file);
                    } else {
                        file.delete();
                    }
                }
            }
            folder.delete();
        }
    }
}