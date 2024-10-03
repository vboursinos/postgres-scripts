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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    public void testSelectCode1() throws SQLException, IOException {
        long startTime = System.currentTimeMillis();
        Main.executeScriptWriteResultFile(connection, "src/main/resources/select-sql/select-code1.sql", "src/main/resources/results/file1.txt");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        File resultFile = new File("src/main/resources/results/file1.txt");
        File correctResultFile = new File("src/main/resources/correct-results/file1.txt");
        assertRowCountAndColumnCount("src/main/resources/results/file1.txt", 6, 70);
        assertCompareSqlResults(resultFile, correctResultFile);
    }

    @Test
    public void testSelectCode2() throws SQLException, IOException {
        long startTime = System.currentTimeMillis();
        Main.executeScriptWriteResultFile(connection, "src/main/resources/select-sql/select-code2.sql", "src/main/resources/results/file2.txt");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        File resultFile = new File("src/main/resources/results/file2.txt");
        File correctResultFile = new File("src/main/resources/correct-results/file2.txt");
        assertRowCountAndColumnCount("src/main/resources/results/file2.txt", 8, 8);
        assertCompareSqlResults(resultFile, correctResultFile);
    }

    @Test
    public void testSelectCode4() throws SQLException, IOException {
        long startTime = System.currentTimeMillis();
        Main.executeScriptWriteResultFile(connection, "src/main/resources/select-sql/select-code4.sql", "src/main/resources/results/file4.txt");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        File resultFile = new File("src/main/resources/results/file4.txt");
        File correctResultFile = new File("src/main/resources/correct-results/file4.txt");
        assertRowCountAndColumnCount("src/main/resources/results/file4.txt", 2, 1);
        assertCompareSqlResults(resultFile, correctResultFile);
    }

    @Test
    public void testSelectCode5() throws SQLException, IOException {
        long startTime = System.currentTimeMillis();
        Main.executeScriptWriteResultFile(connection, "src/main/resources/select-sql/select-code5.sql", "src/main/resources/results/file5.txt");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        File resultFile = new File("src/main/resources/results/file5.txt");
        File correctResultFile = new File("src/main/resources/correct-results/file5.txt");
        assertRowCountAndColumnCount("src/main/resources/results/file5.txt", 2, 1);
        assertCompareSqlResults(resultFile, correctResultFile);
    }


    @Test
    public void testSelectCode7() throws SQLException, IOException {
        long startTime = System.currentTimeMillis();
        Main.executeScriptWriteResultFile(connection, "src/main/resources/select-sql/select-code7.sql", "src/main/resources/results/file7.txt");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        File resultFile = new File("src/main/resources/results/file7.txt");
        File correctResultFile = new File("src/main/resources/correct-results/file7.txt");
        assertRowCountAndColumnCount("src/main/resources/results/file7.txt", 5, 31);
        assertCompareSqlResults(resultFile, correctResultFile);
    }

    @Test
    public void testSelectCode8() throws SQLException, IOException {
        long startTime = System.currentTimeMillis();
        Main.executeScripts(connection, "src/main/resources/select-sql/", "select-code8.sql");
        Main.executeScriptWriteResultFile(connection, "src/main/resources/select-sql/select-code8.sql", "src/main/resources/results/file8.txt");
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        System.out.println("Duration: " + duration + "s");
        File resultFile = new File("src/main/resources/results/file8.txt");
        File correctResultFile = new File("src/main/resources/correct-results/file8.txt");
        assertRowCountAndColumnCount("src/main/resources/results/file8.txt", 5, 31);
        assertCompareSqlResults(resultFile, correctResultFile);
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

    private void assertCompareSqlResults(File resultFile, File correctResultFile) throws IOException {

        Set<String> resultLines = new HashSet<>(Files.readAllLines(resultFile.toPath(), StandardCharsets.UTF_8));
        Set<String> correctResultLines = new HashSet<>(Files.readAllLines(correctResultFile.toPath(), StandardCharsets.UTF_8));

        assertEquals(correctResultLines, resultLines, "The files content are not the same");
        System.out.println("Assertion passed: The files content are the same.");
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
