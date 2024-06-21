import com.github.javafaker.Faker;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MultiThreadedDBInserter {
    private static final int NUM_THREADS = 35;
    static final long TARGET_RECORDS = 10_000_000;
    static final AtomicLong records = new AtomicLong();
    static final AtomicLong populatedRecords = new AtomicLong();
    static final AtomicLong  activeThreads = new AtomicLong();
    static ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

    public static void main(String[] args) throws InterruptedException {
        long startTime = System.currentTimeMillis();

        // Status update thread
        new Thread(() -> {
            try {
                long lastTime = 0, lastRecords = 0;
                while (!executor.isTerminated()) {
                    //System.out.println("Created " + MultiThreadedDBInserter.createdRecords.get() + " records");
                    long timeTaken = System.currentTimeMillis() - lastTime;
                    double transactionsPerMinute = ((MultiThreadedDBInserter.populatedRecords.get() - lastRecords) ) / ((timeTaken / 1000.0) / 60);
                    System.out.println("\nInserted " + MultiThreadedDBInserter.populatedRecords.get() + " records");
                    System.out.println("Transactions Per Minute: " + transactionsPerMinute);
                    System.out.println("Active Threads " + MultiThreadedDBInserter.activeThreads.get());
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    System.out.println("Elapsed Time: " + (elapsedTime / 1000) / 60 + "M " + (elapsedTime / 1000) % 60 + "S");
                    lastTime = System.currentTimeMillis();
                    lastRecords = MultiThreadedDBInserter.populatedRecords.get();
                    Thread.sleep(60000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();

        for (int i = 0; i < TARGET_RECORDS; i++) {
            executor.execute(new Worker());
            TimeUnit.MICROSECONDS.sleep(10);
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.DAYS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        double transactionsPerMinute = (populatedRecords.get() / (timeTaken / 1000.0 / 60));

        System.out.println("\nSuccessfully Populated " + populatedRecords.get() + " records");
        System.out.println("Time Taken: " + timeTaken + " ms");
        System.out.println("Transactions Per Minute: " + transactionsPerMinute);
    }
}

class Worker implements Runnable {
    private static final Random random = new Random();
    private static final Faker faker = new Faker();

    @Override
    public void run() {
        try {
            TimeUnit.MICROSECONDS.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        task();
    }

    public static void task() {
        MultiThreadedDBInserter.activeThreads.incrementAndGet();
        try (Connection conn = Data_Source.getConnection()) {
            conn.setAutoCommit(false);

            String insertEmp = "INSERT INTO employees (name, dob, gender, department_id, employment_type, employment_date) VALUES (?,?,?,?,?,?)";
            String insertCon = "INSERT INTO contact_info (employee_id, address, phone_no, email, emergency_contact_no, emergency_contact_name) VALUES (?,?,?,?,?,?)";
            String insertSal = "INSERT INTO salaries (employee_id, month, basic_salary, total_allowances, total_deductions, total_gross_earnings, tax_relief, tax_relief_description, total_taxes, net_salary) VALUES (?,?,?,?,?,?,?,?,?,?)";
            String insertAllowance = "INSERT INTO allowances (employee_id, month, allowance_name, allowance_description, allowance_rate, allowance_type, allowance_amount) VALUES (?,?,?,?,?,?,?)";
            String insertDeduction = "INSERT INTO deductions (employee_id, month, deduction_name, deduction_description, deduction_type, deduction_amount) VALUES (?,?,?,?,?,?)";
            String insertTax = "INSERT INTO taxes (employee_id, month, gross_salary, tax_name, tax_rate, tax_type, tax_amount) VALUES (?,?,?,?,?,?,?)";
            String insertBank = "INSERT INTO bank_details (employee_id, account_no, bank_name, branch_code) VALUES (?,?,?,?)";

            PreparedStatement pstmtEmp = conn.prepareStatement(insertEmp, PreparedStatement.RETURN_GENERATED_KEYS);
            PreparedStatement pstmtCon = conn.prepareStatement(insertCon);
            PreparedStatement pstmtSal = conn.prepareStatement(insertSal);
            PreparedStatement pstmtAllowance = conn.prepareStatement(insertAllowance);
            PreparedStatement pstmtDeduction = conn.prepareStatement(insertDeduction);
            PreparedStatement pstmtTax = conn.prepareStatement(insertTax);
            PreparedStatement pstmtBank = conn.prepareStatement(insertBank);

            long employeeId = insertEmployee(pstmtEmp);
            double basicSalary = random.nextDouble() * (2000000 - 50000) + 50000;

            for (int j = 0; j < 3; j++) {
                Date month = convertUtilToSqlDate(new java.util.Date(faker.date().past(365, TimeUnit.DAYS).getTime()));
                double totalAllowances = 0.0;
                totalAllowances += insertAllowances(pstmtAllowance, employeeId, month, basicSalary, 0.03, "House Allowance");
                totalAllowances += insertAllowances(pstmtAllowance, employeeId, month, basicSalary, 0.015, "Transport Allowance");
                totalAllowances += insertAllowances(pstmtAllowance, employeeId, month, basicSalary, 0.02, "Mortgage Allowance");
                double totalDeductions = insertDeductions(pstmtDeduction, employeeId, month);
                double grossSalary = basicSalary + totalAllowances - totalDeductions;
                double totalTaxes = insertTaxes(pstmtTax, employeeId, month, grossSalary, "PAYE");

                insertSalaryDetails(pstmtSal, employeeId, month, basicSalary, totalAllowances, totalDeductions, grossSalary, totalTaxes);
                basicSalary = basicSalary * 1.02;
            }

            insertContactInfo(pstmtCon, employeeId);
            insertBankDetails(pstmtBank, employeeId);
            conn.commit();

            pstmtEmp.close();
            pstmtCon.close();
            pstmtSal.close();
            pstmtAllowance.close();
            pstmtDeduction.close();
            pstmtTax.close();
            pstmtBank.close();

            MultiThreadedDBInserter.activeThreads.decrementAndGet();
            MultiThreadedDBInserter.populatedRecords.incrementAndGet();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static long insertEmployee(PreparedStatement pstmt) throws SQLException {
        pstmt.setString(1, faker.name().fullName());
        pstmt.setDate(2, new java.sql.Date(faker.date().birthday().getTime()));
        pstmt.setString(3, faker.options().option("M", "F"));
        pstmt.setLong(4, (long) (Math.random() * 10) + 1);
        pstmt.setString(5, "FULL-TIME");
        pstmt.setDate(6, new java.sql.Date(faker.date().past(10 * 365, TimeUnit.DAYS).getTime()));

        pstmt.executeUpdate();


        try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
            if (generatedKeys.next()) {
                return generatedKeys.getLong(1);
            } else {
                throw new SQLException("Failed to retrieve generated employee ID");
            }
        }
    }

    private static void insertContactInfo(PreparedStatement pstmt, long employeeId) throws SQLException {
        pstmt.setLong(1, employeeId);
        pstmt.setString(2, faker.address().fullAddress());
        pstmt.setString(3, faker.phoneNumber().cellPhone());
        pstmt.setString(4, faker.internet().emailAddress());
        pstmt.setString(5, faker.phoneNumber().cellPhone());
        pstmt.setString(6, faker.name().fullName());
        pstmt.executeUpdate();
    }

    private static double insertAllowances(PreparedStatement pstmt, long employeeId, Date month, double basicSalary, double rate, String allowanceName) throws SQLException {
        pstmt.setLong(1, employeeId);
        pstmt.setDate(2, month);
        pstmt.setString(3, allowanceName);
        pstmt.setString(4, "Monthly: " + allowanceName);
        pstmt.setDouble(5, rate);
        pstmt.setString(6, "PERCENTAGE");
        pstmt.setDouble(7, basicSalary * rate);
        pstmt.executeUpdate();
        return basicSalary * rate;
    }

    private static double insertDeductions(PreparedStatement pstmt, long employeeId, Date month) throws SQLException {
        pstmt.setLong(1, employeeId);
        pstmt.setDate(2, month);
        pstmt.setString(3, "Pension Contribution");
        pstmt.setString(4, "Monthly pension contribution");
        pstmt.setString(5, "FIXED");
        pstmt.setDouble(6, 5000.0);
        pstmt.executeUpdate();
        return 5000.0;
    }

    private static double insertTaxes(PreparedStatement pstmt, long employeeId, Date month, double grossSalary, String taxName) throws SQLException {
        pstmt.setLong(1, employeeId);
        pstmt.setDate(2, month);
        pstmt.setDouble(3, grossSalary);
        pstmt.setString(4, taxName);
        pstmt.setDouble(5, 0.14);
        pstmt.setString(6, "PERCENTAGE");

        double taxAmount = grossSalary * 0.14;
        pstmt.setDouble(7, taxAmount);
        pstmt.executeUpdate();
        return taxAmount;
    }

    private static void insertSalaryDetails(PreparedStatement pstmt, long employeeId, Date month, double basicSalary, double totalAllowances, double totalDeductions, double grossSalary, double totalTaxes) throws SQLException {
        pstmt.setLong(1, employeeId);
        pstmt.setDate(2, month);
        pstmt.setDouble(3, basicSalary);
        pstmt.setDouble(4, totalAllowances);
        pstmt.setDouble(5, totalDeductions);
        pstmt.setDouble(6, grossSalary);
        pstmt.setDouble(7, 0);
        pstmt.setString(8, "");
        pstmt.setDouble(9, totalTaxes);
        pstmt.setDouble(10, grossSalary - totalTaxes);
        pstmt.executeUpdate();
    }

    private static void insertBankDetails(PreparedStatement pstmt, long employeeId) throws SQLException {
        pstmt.setLong(1, employeeId);
        pstmt.setString(2, faker.finance().iban());
        pstmt.setString(3, faker.company().name());
        pstmt.setString(4, faker.address().zipCode());
        pstmt.executeUpdate();
    }

    private static Date convertUtilToSqlDate(java.util.Date date) {
        return new java.sql.Date(date.getTime());
    }
}
