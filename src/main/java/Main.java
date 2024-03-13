import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

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
            String sqlFilePath = "src/main/resources/files/init_tables.sql";
            BufferedReader reader = new BufferedReader(new FileReader(sqlFilePath));
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append("\n");
            }
            reader.close();
            String sqlQuery = stringBuilder.toString();

            // Create statement
            Statement statement = connection.createStatement();

            // Execute SQL script
            statement.execute(sqlQuery);

            // Close connections
            statement.close();
            connection.close();

            System.out.println("SQL script executed successfully.");

        } catch (Exception e) {
            System.err.println("Error executing SQL script: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
