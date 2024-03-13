import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        try {
            Properties properties = loadProperties("src/main/resources/configuration.properties");

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
            }
        } catch (IOException | SQLException | ClassNotFoundException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static Properties loadProperties(String filePath) throws IOException {
        try (FileReader fileReader = new FileReader(filePath)) {
            Properties properties = new Properties();
            properties.load(fileReader);
            return properties;
        }
    }

    public static void executeScript(Connection connection, String filePath) throws SQLException {
        try (Statement statement = connection.createStatement();
             BufferedReader reader = new BufferedReader(new FileReader(filePath))) {

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
        } catch (IOException e) {
            throw new SQLException("Error reading SQL script: " + e.getMessage());
        }
    }

    public static void executeScripts(Connection connection, String sqlFilePath, String... fileNames) throws SQLException {
        for (String fileName : fileNames) {
            executeScript(connection, sqlFilePath + fileName);
        }
    }
}
