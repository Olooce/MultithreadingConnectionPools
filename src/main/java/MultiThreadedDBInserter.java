import com.github.javafaker.Faker;
import java.sql.*;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MultiThreadedDBInserter {
    private static final int NUM_THREADS = 16;
    private static final int MAX = 5000;
    private static final int MIN = 1000;
    private static final long TARGET_RECORDS = 1000;
    private static final AtomicLong records = new AtomicLong();
    private static final AtomicLong populatedRecords = new AtomicLong();
    private static final Faker faker = new Faker();

    public static void main(String[] args) {
        Random random = new Random();
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        int numRecordsForThread;

        try {
            while (records.get() < TARGET_RECORDS) {
                numRecordsForThread = Math.min(random.nextInt(MAX - MIN + 1) + MIN, (int) (TARGET_RECORDS - records.get()));
                executor.execute(task(numRecordsForThread));
                records.getAndAdd(numRecordsForThread);
            }
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.DAYS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static Runnable task(int numRecordsForThread) {
        return () -> {
            try (Connection conn = Data_Source.getConnection()) {
                conn.setAutoCommit(false);
                for (int i = 0; i < numRecordsForThread; i++) {
                    long employeeId = insertEmployee(conn);
                    insertContactInfo(conn, employeeId);
                    insertSalaries(conn, employeeId);
                    insertBankDetails(conn, employeeId);
                    populatedRecords.incrementAndGet();
                    if (populatedRecords.get() % 1000 == 0) {
                        System.out.println("Populated " + populatedRecords.get() + " records");
                        conn.commit();
                    }
                }
                conn.commit();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        };
    }

    private static void insertSalaries(Connection conn, long employeeId) throws SQLException {
        Date month = new Date(faker.date().past(365, TimeUnit.DAYS).getTime());
        double basicSalary = faker.number().randomDouble(2, 50000, 2000000);

        for(int y = 0; y < 3; y++){
            double totalAllowances = insertAllowances(conn, employeeId, month, basicSalary);
            double totalDeductions = insertDeductions(conn, employeeId, month);
            double grossSalary = basicSalary + totalAllowances - totalDeductions;
            double total_taxes = insertTaxes(conn, employeeId, month, grossSalary);

            insertSalaryDetails(conn, employeeId, month, basicSalary, totalAllowances, totalDeductions, grossSalary, total_taxes);
            basicSalary = basicSalary * 1.02;
        }
    }

    private static double insertTaxes(Connection conn, long employeeId, Date month, double grossSalary) throws SQLException {
        String sql = "INSERT INTO taxes (employee_id, month, gross_salary, tax_name, tax_rate, tax_type, tax_amount) VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, employeeId);
            pstmt.setDate(2, month);
            pstmt.setDouble(3, grossSalary);
            pstmt.setString(4, "PAYE");
            pstmt.setDouble(5, 0.14); // Assuming a 14% tax rate
            pstmt.setString(6, "PERCENTAGE");
            pstmt.setDouble(7, grossSalary * 0.14);
            pstmt.executeUpdate();
        }
        return grossSalary * 0.14;
    }

    private static double insertDeductions(Connection conn, long employeeId, Date month) throws SQLException {
        String sql = "INSERT INTO deductions (employee_id, month, deduction_name, deduction_description, deduction_type, deduction_amount) VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, employeeId);
            pstmt.setDate(2, month);
            pstmt.setString(3, "HEALTH INSURANCE");
            pstmt.setString(4, faker.lorem().sentence());
            pstmt.setString(5, "FIXED");
            double deductionAmount = faker.number().randomDouble(2, 500, 2000);
            pstmt.setDouble(6, deductionAmount);
            pstmt.executeUpdate();
            return deductionAmount;
        }
    }

    private static double insertAllowances(Connection conn, long employeeId, Date month, double basicSalary) throws SQLException {
        double totalAllowances = 0.0;
        totalAllowances += insertAllowance(conn, employeeId, month, "House Allowance", 0.03, basicSalary);
        totalAllowances += insertAllowance(conn, employeeId, month, "Transport Allowance", 0.015, basicSalary);
        totalAllowances += insertAllowance(conn, employeeId, month, "Mortgage Allowance", 0.02, basicSalary);
        return totalAllowances;
    }

    private static double insertAllowance(Connection conn, long employeeId, Date month, String allowanceName, double rate, double basicSalary) throws SQLException {
        String sql = "INSERT INTO allowances (employee_id, month, allowance_name, allowance_description, allowance_rate, allowance_type, allowance_amount) VALUES (?, ?, ?, ?, ?, ?, ?)";
        double allowanceAmount = basicSalary * rate;
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, employeeId);
            pstmt.setDate(2, month);
            pstmt.setString(3, allowanceName);
            pstmt.setString(4, faker.lorem().sentence());
            pstmt.setDouble(5, rate);
            pstmt.setString(6, "PERCENTAGE");
            pstmt.setDouble(7, allowanceAmount);
            pstmt.executeUpdate();
        }
        return allowanceAmount;
    }

    private static void insertSalaryDetails(Connection conn, long employeeId, Date month, double basicSalary, double totalAllowances, double totalDeductions, double grossSalary, double totalTaxes) throws SQLException {
        String sql = "INSERT INTO salaries (employee_id, month, basic_salary, total_allowances, total_deductions, total_gross_earnings, tax_relief, tax_relief_description, total_taxes, net_salary) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, employeeId);
            pstmt.setDate(2, month);
            pstmt.setDouble(3, basicSalary);
            pstmt.setDouble(4, totalAllowances);
            pstmt.setDouble(5, totalDeductions);
            pstmt.setDouble(6, grossSalary);
            pstmt.setDouble(7, 0); // tax_relief
            pstmt.setString(8, ""); // tax_relief_description
            pstmt.setDouble(9, totalTaxes);
            pstmt.setDouble(10, grossSalary - totalTaxes); // net_salary

            pstmt.executeUpdate();
        }
    }

    private static void insertContactInfo(Connection conn, long employeeId) throws SQLException {
        String sql = "INSERT INTO contact_info (employee_id, address, phone_no, email, emergency_contact_no, emergency_contact_name) VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, employeeId);
            pstmt.setString(2, faker.address().fullAddress());
            pstmt.setString(3, faker.phoneNumber().cellPhone());
            pstmt.setString(4, faker.internet().emailAddress());
            pstmt.setString(5, faker.phoneNumber().cellPhone());
            pstmt.setString(6, faker.name().fullName());
            pstmt.executeUpdate();
        }
    }

    private static long insertEmployee(Connection conn) throws SQLException {
        String sql = "INSERT INTO employees (name, dob, gender, department_id, employment_type, employment_date) VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
            pstmt.setString(1, faker.name().fullName());
            pstmt.setDate(2, new Date(faker.date().birthday().getTime()));
            pstmt.setString(3, faker.options().option("M", "F"));
            pstmt.setLong(4, faker.number().randomNumber(1, true));
            pstmt.setString(5, "FULL-TIME");
            pstmt.setDate(6, new Date(faker.date().past(10 * 365, TimeUnit.DAYS).getTime()));
            pstmt.executeUpdate();
            ResultSet generatedKeys = pstmt.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getLong(1);
            } else {
                throw new SQLException("Creating employee failed, no ID obtained.");
            }
        }
    }

    private static void insertBankDetails(Connection conn, long employeeId) throws SQLException {
        String sql = "INSERT INTO bank_details (employee_id, account_no, bank_name, branch_code) VALUES (?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, employeeId);
            pstmt.setString(2, faker.finance().iban());
            pstmt.setString(3, faker.company().name());
            pstmt.setString(4, faker.finance().bic());
            pstmt.executeUpdate();
        }
    }
}
