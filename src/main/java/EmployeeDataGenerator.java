import com.github.javafaker.Faker;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class EmployeeDataGenerator {

    private static final int NUM_THREADS = 10;
    private static final int BATCH_SIZE = 100000;

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        Faker faker = new Faker();
        Random random = new Random();

        try (Connection conn = ConnectDB.getConnection()) {
            conn.setAutoCommit(false);

            for (int i = 0; i < NUM_THREADS; i++) {
                executor.submit(() -> {
                    generateEmployeeData(conn, faker, random);
                });
            }

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            conn.commit();
        } catch (InterruptedException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static void generateEmployeeData(Connection conn, Faker faker, Random random) {
        try {
            for (int i = 0; i < BATCH_SIZE; i++) {
                String employeeId = "EMP" + String.format("%07d", ThreadLocalRandom.current().nextInt(1, 10000000 + 1));
                String name = faker.name().fullName();
                LocalDate dob = faker.date().birthday().toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDate();
                String gender = faker.options().option("Male", "Female");
                long departmentId = random.nextInt(5) + 1;
                String employmentType = faker.options().option("FULL-TIME", "PART-TIME", "CONTRACT", "TEMPORARY");

                LocalDate baseDate = LocalDate.now().minusDays(random.nextInt(365));
                LocalDate employmentDate = baseDate.plusDays(random.nextInt(365));

                insertEmployee(conn, employeeId, name, dob, gender, departmentId, employmentType, employmentDate);
                generateAndInsertSalaries(conn, employeeId, employmentDate);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertEmployee(Connection conn, String employeeId, String name, LocalDate dob, String gender,
                                       long departmentId, String employmentType, LocalDate employmentDate) throws SQLException {
        String sql = "INSERT INTO employees (employee_id, name, dob, gender, department_id, employment_type, employment_date, status) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, employeeId);
            pstmt.setString(2, name);
            pstmt.setDate(3, Date.valueOf(dob)); // Convert LocalDate to java.sql.Date
            pstmt.setString(4, gender);
            pstmt.setLong(5, departmentId);
            pstmt.setString(6, employmentType);
            pstmt.setDate(7, Date.valueOf(employmentDate)); // Convert LocalDate to java.sql.Date
            pstmt.setString(8, "NEW"); // Initially all employees are NEW
            pstmt.executeUpdate();
        }
    }

    private static void generateAndInsertSalaries(Connection conn, String employeeId, LocalDate employmentDate) throws SQLException {
        LocalDate currentDate = employmentDate;
        BigDecimal basicSalary = BigDecimal.valueOf(50000); // Example starting basic salary
        BigDecimal houseAllowance;
        BigDecimal transportAllowance;
        BigDecimal mortgageAllowance;
        BigDecimal totalGrossEarnings;
        BigDecimal payeTax;
        BigDecimal netSalary;

        String sql = "INSERT INTO salaries (employee_id, month, basic_salary, total_gross_earnings, paye_tax, net_salary) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int month = 1; month <= 3; month++) {
                houseAllowance = basicSalary.multiply(BigDecimal.valueOf(0.03));
                transportAllowance = basicSalary.multiply(BigDecimal.valueOf(0.015));
                mortgageAllowance = basicSalary.multiply(BigDecimal.valueOf(0.02));
                totalGrossEarnings = basicSalary.add(houseAllowance).add(transportAllowance).add(mortgageAllowance);
                payeTax = totalGrossEarnings.multiply(BigDecimal.valueOf(0.14));
                netSalary = totalGrossEarnings.subtract(payeTax);

                pstmt.setString(1, employeeId);
                pstmt.setDate(2, Date.valueOf(currentDate.withDayOfMonth(1))); // First day of the month
                pstmt.setBigDecimal(3, basicSalary.setScale(2, RoundingMode.HALF_UP));
                pstmt.setBigDecimal(4, totalGrossEarnings.setScale(2, RoundingMode.HALF_UP));
                pstmt.setBigDecimal(5, payeTax.setScale(2, RoundingMode.HALF_UP));
                pstmt.setBigDecimal(6, netSalary.setScale(2, RoundingMode.HALF_UP));
                pstmt.executeUpdate();

                // Increase basic salary by 2% for next month
                basicSalary = basicSalary.multiply(BigDecimal.valueOf(1.02));
                currentDate = currentDate.plusMonths(1);
            }
        }
    }
}
