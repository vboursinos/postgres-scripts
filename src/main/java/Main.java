import java.io.*;
import java.sql.*;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream("src/main/resources/configuration.properties")) {
            properties.load(input);

            String url = properties.getProperty("POSTGRES_URL");
            String user = properties.getProperty("POSTGRES_USER");
            String password = properties.getProperty("POSTGRES_PASSWORD");
            String sqlFilePath = properties.getProperty("SQL_FILE_PATH");

            try (Connection connection = DriverManager.getConnection(url, user, password)) {
                Class.forName("org.postgresql.Driver");

                executeScript(connection, sqlFilePath + "init_tables.sql");
                executeScripts(connection, sqlFilePath,
                        "code1.sql", "code2.sql", "code4.sql", "code5.sql", "code7.sql", "code8.sql");

                System.out.println("Data insertion completed successfully.");

            } catch (Exception e) {
                System.err.println("Error executing SQL script: " + e.getMessage());
                e.printStackTrace();
            }
        } catch (IOException e) {
            System.err.println("Error reading properties file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void executeScript(Connection connection, String filePath) throws SQLException {
        try (Statement statement = connection.createStatement();
             FileInputStream input = new FileInputStream(filePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {

            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append("\n");
            }
            String[] sqlStatements = stringBuilder.toString().split(";");

            for (String sql : sqlStatements) {
                if (!sql.trim().isEmpty()) {
                    statement.execute(sql);
                }
            }
        } catch (Exception e) {
            throw new SQLException("Error executing SQL script: " + e.getMessage());
        }
    }

    private static void executeScripts(Connection connection, String sqlFilePath, String... fileNames) throws SQLException {
        for (String fileName : fileNames) {
            executeScript(connection, sqlFilePath + fileName);
        }
    }
}