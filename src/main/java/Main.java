import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;

public class Main {

    private static final String URL = "jdbc:postgresql://localhost:5432/mydatabase";
    private static final String USER = "myuser";
    private static final String PASSWORD = "secret";
    private static final String SQL_FILE_PATH = "src/main/resources/sql/";

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            Class.forName("org.postgresql.Driver");

            executeScript(connection, "init_tables.sql");
            executeScripts(connection, "code1.sql", "code2.sql", "code4.sql", "code5.sql", "code7.sql", "code8.sql");

            System.out.println("Data insertion completed successfully.");

        } catch (Exception e) {
            System.err.println("Error executing SQL script: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void executeScript(Connection connection, String fileName) throws SQLException {
        String filePath = SQL_FILE_PATH + fileName;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append("\n");
            }
            String[] sqlStatements = stringBuilder.toString().split(";");

            try (Statement statement = connection.createStatement()) {
                for (String sql : sqlStatements) {
                    if (!sql.trim().isEmpty()) {
                        statement.execute(sql);
                    }
                }
            }
        } catch (Exception e) {
            throw new SQLException("Error executing SQL script: " + e.getMessage());
        }
    }

    private static void executeScripts(Connection connection, String... fileNames) throws SQLException {
        for (String fileName : fileNames) {
            executeScript(connection, fileName);
        }
    }

}
