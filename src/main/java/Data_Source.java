import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class Data_Source {

    private static final BasicDataSource dataSource;

    static {
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/mwm_pwm");
        dataSource.setUsername("root");
        dataSource.setPassword("Brook_side_96");
        dataSource.setInitialSize(5);
        dataSource.setMaxTotal(20);
        dataSource.setMinIdle(3);
        dataSource.setMaxIdle(10);
    }

    public static DataSource getDataSource() {
        return dataSource;
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static void closeDataSource() throws SQLException {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
