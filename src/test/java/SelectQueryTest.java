import io.github.cdimascio.dotenv.Dotenv;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SelectQueryTest {
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
    public void testSelectCode1() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScriptWriteResultFile(connection, "src/main/resources/select-sql/select-code1.sql", "src/main/resources/results/file1.txt");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");

        assertRowCountAndColumnCount("src/main/resources/results/file1.txt", 6, 70);
    }

    @Test
    public void testSelectCode2() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScriptWriteResultFile(connection, "src/main/resources/select-sql/select-code2.sql", "src/main/resources/results/file2.txt");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        assertRowCountAndColumnCount("src/main/resources/results/file2.txt", 8, 8);
    }

    @Test
    public void testSelectCode4() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScriptWriteResultFile(connection, "src/main/resources/select-sql/select-code4.sql", "src/main/resources/results/file4.txt");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        assertRowCountAndColumnCount("src/main/resources/results/file4.txt", 2, 1);
    }

    @Test
    public void testSelectCode5() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScriptWriteResultFile(connection, "src/main/resources/select-sql/select-code5.sql", "src/main/resources/results/file5.txt");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        assertRowCountAndColumnCount("src/main/resources/results/file5.txt", 2, 1);
    }


    @Test
    public void testSelectCode7() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScriptWriteResultFile(connection, "src/main/resources/select-sql/select-code7.sql", "src/main/resources/results/file7.txt");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        assertRowCountAndColumnCount("src/main/resources/results/file7.txt", 5, 31);
    }

    @Test
    public void testSelectCode8() throws SQLException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/select-sql/", "select-code8.sql");
        Main.executeScriptWriteResultFile(connection, "src/main/resources/select-sql/select-code8.sql", "src/main/resources/results/file8.txt");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        assertRowCountAndColumnCount("src/main/resources/results/file8.txt", 5, 31);
    }

    private void assertRowCountAndColumnCount(String filename, int expectedColumnCount, int expectedRowCount) throws SQLException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            int totalRowCount = 0;
            int totalColumnCount = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(",");
                totalColumnCount = Math.max(totalColumnCount, columns.length);
                totalRowCount++;
            }

            assertEquals(expectedColumnCount, totalColumnCount);

            assertEquals(expectedRowCount, totalRowCount);

            System.out.println("Assertion passed: Row count and Column count match the expected values.");
        } catch (IOException e) {
            throw new SQLException("Error reading file: " + e.getMessage());
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
