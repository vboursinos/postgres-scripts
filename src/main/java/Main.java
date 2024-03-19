import io.github.cdimascio.dotenv.Dotenv;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        try {
            Dotenv dotenv = Dotenv.configure().load();
            String url = dotenv.get("POSTGRES_URL");
            String user = dotenv.get("POSTGRES_USER");
            String password = dotenv.get("POSTGRES_PASSWORD");

            Properties properties = loadProperties("src/main/resources/configuration.properties");
            String sqlFilePath = properties.getProperty("SQL_FILE_PATH");
            String csvFilePath = properties.getProperty("CSV_FILE_PATH");

            try (Connection connection = DriverManager.getConnection(url, user, password)) {
                Class.forName("org.postgresql.Driver");

                executeScript(connection, sqlFilePath + "init_tables.sql");
                CopyManager copyManager = new CopyManager((BaseConnection) connection);
                insertData(Paths.get(csvFilePath,"tab_c_gt.csv").toString(), "demo.tab_c_gt", copyManager);
                insertData(Paths.get(csvFilePath,"tab_oab.csv").toString(), "demo.tab_oab", copyManager);
                insertData(Paths.get(csvFilePath,"tab_sbfa.csv").toString(), "demo.tab_sbfa", copyManager);
                insertData(Paths.get(csvFilePath,"tab_t_5_c_c_1.csv").toString(), "demo.tab_t_5_c_c_1", copyManager);

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

    public static void executeScriptWriteResultFile(Connection connection, String filePath, String outputFilePath) throws SQLException {
        try (Statement statement = connection.createStatement();
             BufferedReader reader = new BufferedReader(new FileReader(filePath));
             FileWriter writer = new FileWriter(outputFilePath)) {

            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append("\n");
            }
            String[] sqlStatements = stringBuilder.toString().split(";");

            for (String sql : sqlStatements) {
                if (!sql.trim().isEmpty()) {
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
                }
            }
        } catch (IOException e) {
            throw new SQLException("Error reading SQL script: " + e.getMessage());
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
            System.out.println("Script " + fileName + " executed successfully.");
        }
    }

    public static void insertData (String csvFilePath , String tableName, CopyManager copyManager) throws IOException, SQLException {
        FileReader fileReader = new FileReader(csvFilePath);
        copyManager.copyIn("COPY " + tableName + " FROM STDIN WITH CSV HEADER DELIMITER ','", fileReader);

        System.out.println("Data imported successfully from CSV file: " + csvFilePath);

    }
}
