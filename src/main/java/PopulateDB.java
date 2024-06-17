import com.github.javafaker.Faker;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PopulateDB {
    public static void main(String[] args) throws SQLException {

        try (Connection conn = ConnectDB.getConnection(); PreparedStatement pstmt = conn.prepareStatement("INSERT INTO employees (name, dob, gender, department_id, employment_type, employment_date) VALUES (?, ?, ?, ?, ?, ?)")) {
            Faker faker = new Faker();
            for (int i = 0; i < 1000000; i++) {
                pstmt.setString(1, faker.name().fullName());
                pstmt.setDate(2, new java.sql.Date(faker.date().birthday().getTime()));
                pstmt.setString(3, faker.options().option("Male", "Female"));
                pstmt.setLong(4, (long) (Math.random() * 10) + 1); // Assign a random department ID
                pstmt.setString(5, "FULL-TIME"); // Assign a default employment type
                pstmt.setDate(6, new java.sql.Date(faker.date().past(10, TimeUnit.YEARS).getTime())); // Assign a default employment date
                pstmt.executeUpdate();
            }
            conn.commit();
        }

    }
}

