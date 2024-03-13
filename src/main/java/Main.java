import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;

public class Main {

    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/mydatabase";
        String user = "myuser";
        String password = "secret";

        try {
            // Load the PostgreSQL JDBC driver
            Class.forName("org.postgresql.Driver");

            // Establish connection to the database
            Connection connection = DriverManager.getConnection(url, user, password);

            // Read SQL file
            String sqlFilePath = "src/main/resources/sql/init_tables.sql";
            executeScript(connection, sqlFilePath);

            String code1sqlFilePath = "src/main/resources/sql/code1.sql";
            executeScript(connection, code1sqlFilePath);

            String code2sqlFilePath = "src/main/resources/sql/code2.sql";
            executeScript(connection, code2sqlFilePath);

//            String code3sqlFilePath = "src/main/resources/sql/code3.sql";
//            executeScript(connection, code3sqlFilePath);

            String code4sqlFilePath = "src/main/resources/sql/code4.sql";
            executeScript(connection, code4sqlFilePath);

            String code5sqlFilePath = "src/main/resources/sql/code5.sql";
            executeScript(connection, code5sqlFilePath);

            String code7sqlFilePath = "src/main/resources/sql/code7.sql";
            executeScript(connection, code7sqlFilePath);

            String code8sqlFilePath = "src/main/resources/sql/code8.sql";
            executeScript(connection, code8sqlFilePath);

            // Close connection
            connection.close();

            System.out.println("Data insertion completed successfully.");

        } catch (Exception e) {
            System.err.println("Error executing SQL script: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void executeScript(Connection connection, String scriptFilePath) throws SQLException {
        try (BufferedReader reader = new BufferedReader(new FileReader(scriptFilePath))) {
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append("\n");
            }
            String[] sqlStatements = stringBuilder.toString().split(";");

            // Create statement
            Statement statement = connection.createStatement();

            // Execute each SQL statement
            for (String sql : sqlStatements) {
                if (!sql.trim().isEmpty()) {
                    statement.execute(sql);
                }
            }
            statement.close();
        } catch (Exception e) {
            throw new SQLException("Error executing SQL script: " + e.getMessage());
        }
    }

}
