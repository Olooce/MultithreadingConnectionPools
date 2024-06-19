import org.apache.commons.dbcp2.BasicDataSource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class dataSource {
    private static BasicDataSource dataS;

    static {
        dataS = new BasicDataSource();
        dataS.setDriverClassName("com.mysql.jdbc.Driver");
        dataS.setUrl("jdbc:mysql://localhost:3306/mwm_pms_db");
        dataS.setUsername("root");
        dataS.setPassword("Brook_side_96");

        dataS.setInitialSize(5);
        dataS.setMaxTotal(20);
        dataS.setMinIdle(3);
    }

    public static DataSource getDataSource() {
        return dataS;
    }

    public static Connection getConnection() throws SQLException {
        return dataS.getConnection();
    }

    public static void closeDataSource() throws SQLException {
        if (dataS != null) {
            dataS.close();
        }
    }
}
