import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectDB {
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/mwm_pms_db";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "Brook_side_96";

    private static Connection connection;

    public static Connection getConnection()  {
        try {
            connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
    }


