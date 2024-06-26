import com.github.javafaker.Faker;

import java.sql.*;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MultiThreadedDBInserter {
    private static final int NUM_THREADS = 35; // Number of threads
    static final int BATCH_SIZE = 100; // Batch size
    static final long TARGET_RECORDS = 10_000_000; // Target number of records
    static final AtomicLong populatedRecords = new AtomicLong(); // Populated records counter
    static final AtomicLong activeThreads = new AtomicLong(); // Active threads counter
    static ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS); // Thread pool executorp

    public static void main(String[] args) throws InterruptedException {
        long startTime = System.currentTimeMillis(); // Start time for the process

        // Status update thread
        new Thread(() -> {
            try {
                long lastTime = startTime;
                long lastRecords = 0;

                while (!executor.isTerminated()) {
                    long currentTime = System.currentTimeMillis();
                    long elapsedTime = currentTime - startTime;
                    long timeTaken = currentTime - lastTime;

                    double totalTransactionsPerMinute = (MultiThreadedDBInserter.populatedRecords.get() / (elapsedTime / 1000.0 / 60));
                    double intervalTransactionsPerMinute = ((MultiThreadedDBInserter.populatedRecords.get() - lastRecords) / (timeTaken / 1000.0 / 60));

                    long elapsedSeconds = elapsedTime / 1000;
                    long hours = elapsedSeconds / 3600;
                    long minutes = (elapsedSeconds % 3600) / 60;
                    long seconds = elapsedSeconds % 60;

                    System.out.println("\nInserted " + MultiThreadedDBInserter.populatedRecords.get() + " records");
                    System.out.println("Transactions Per Minute (Total): " + totalTransactionsPerMinute);
                    System.out.println("Transactions Per Minute (Interval): " + intervalTransactionsPerMinute);
                    System.out.println("Active Threads: " + MultiThreadedDBInserter.activeThreads.get());
                    System.out.println("Elapsed Time: " + hours + "H " + minutes + "M " + seconds + "S");

                    lastTime = currentTime;
                    lastRecords = MultiThreadedDBInserter.populatedRecords.get();
                    Thread.sleep(5000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();



        // Submit tasks to the executor
        for (int i = 0; i < TARGET_RECORDS / BATCH_SIZE; i++) {
            executor.execute(new Worker());
            TimeUnit.MICROSECONDS.sleep(500);
        }

        // Shutdown the executor and await termination
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.DAYS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Final statistics
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
        task();
    }

    public static void task() {
        MultiThreadedDBInserter.activeThreads.incrementAndGet();
        try (Connection conn = Data_Source.getConnection()) {
            conn.setAutoCommit(false);

            String insertEmp = "INSERT INTO employees (name, dob, gender, department_id, employment_type, employment_date) VALUES (?,?,?,?,?,?)";
            //String insertCon = "INSERT INTO contact_info (employee_id, address, phone_no, email, emergency_contact_no, emergency_contact_name) VALUES (?,?,?,?,?,?)";
            String insertSal = "INSERT INTO salaries (employee_id, month, basic_salary, total_allowances, total_deductions, total_gross_earnings, total_taxes, net_salary) VALUES (?,?,?,?,?,?,?,?)";
            String insertAllowance = "INSERT INTO allowances (employee_id, month, allowance_name, allowance_description, allowance_rate, allowance_type, allowance_amount) VALUES (?,?,?,?,?,?,?)";
            //String insertDeduction = "INSERT INTO deductions (employee_id, month, deduction_name, deduction_description, deduction_type, deduction_amount) VALUES (?,?,?,?,?,?)";
            String insertTax = "INSERT INTO taxes (employee_id, month, gross_salary, tax_name, tax_rate, tax_type, tax_amount) VALUES (?,?,?,?,?,?,?)";
           // String insertBank = "INSERT INTO bank_details (employee_id, account_no, bank_name, branch_code) VALUES (?,?,?,?)";

            PreparedStatement pstmtEmp = conn.prepareStatement(insertEmp, PreparedStatement.RETURN_GENERATED_KEYS);
           // PreparedStatement pstmtCon = conn.prepareStatement(insertCon);
            PreparedStatement pstmtSal = conn.prepareStatement(insertSal);
            PreparedStatement pstmtAllowance = conn.prepareStatement(insertAllowance);
            //PreparedStatement pstmtDeduction = conn.prepareStatement(insertDeduction);
            PreparedStatement pstmtTax = conn.prepareStatement(insertTax);
           // PreparedStatement pstmtBank = conn.prepareStatement(insertBank);

            for (int i = 0; i < MultiThreadedDBInserter.BATCH_SIZE; i++) {
                long employeeId = insertEmployee(pstmtEmp);
                double basicSalary = random.nextDouble() * (200000 - 50000) + 50000;

                for (int j = 0; j < 3; j++) {
                    Date month = convertUtilToSqlDate(new java.util.Date(faker.date().past(365, TimeUnit.DAYS).getTime()));
                    double totalAllowances = 0.0;
                    totalAllowances += insertAllowances(pstmtAllowance, employeeId, month, basicSalary, 0.03, "House Allowance");
                    totalAllowances += insertAllowances(pstmtAllowance, employeeId, month, basicSalary, 0.015, "Transport Allowance");
                    totalAllowances += insertAllowances(pstmtAllowance, employeeId, month, basicSalary, 0.02, "Mortgage Allowance");
                    double totalDeductions = 0;//insertDeductions(pstmtDeduction, employeeId, month);
                    double grossSalary = basicSalary + totalAllowances - totalDeductions;
                    double totalTaxes = insertTaxes(pstmtTax, employeeId, month, grossSalary, "PAYE");

                    insertSalaryDetails(pstmtSal, employeeId, month, basicSalary, totalAllowances, totalDeductions, grossSalary, totalTaxes);
                    basicSalary = basicSalary * 1.02;
                }

                //insertContactInfo(pstmtCon, employeeId);
                //insertBankDetails(pstmtBank, employeeId);

            }

            pstmtEmp.executeBatch();
            //pstmtCon.executeBatch();
            pstmtSal.executeBatch();
            pstmtAllowance.executeBatch();
            //pstmtDeduction.executeBatch();
            pstmtTax.executeBatch();
            //pstmtBank.executeBatch();

            conn.commit();

            pstmtEmp.close();
           // pstmtCon.close();
            pstmtSal.close();
            pstmtAllowance.close();
            //pstmtDeduction.close();
            pstmtTax.close();
            //pstmtBank.close();
            MultiThreadedDBInserter.populatedRecords.getAndAdd(MultiThreadedDBInserter.BATCH_SIZE);
            MultiThreadedDBInserter.activeThreads.decrementAndGet();
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

        pstmt.addBatch();
        pstmt.executeBatch();

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
        pstmt.addBatch();
    }

    private static double insertAllowances(PreparedStatement pstmt, long employeeId, Date month, double basicSalary, double rate, String allowanceName) throws SQLException {
        pstmt.setLong(1, employeeId);
        pstmt.setDate(2, month);
        pstmt.setString(3, allowanceName);
        pstmt.setString(4, "Monthly: " + allowanceName);
        pstmt.setDouble(5, rate);
        pstmt.setString(6, "PERCENTAGE");
        double allowanceAmount = basicSalary * rate;
        pstmt.setDouble(7, allowanceAmount);
        pstmt.addBatch();
        return allowanceAmount;
    }

    private static double insertDeductions(PreparedStatement pstmt, long employeeId, Date month) throws SQLException {
        double deductionAmount = faker.number().randomDouble(2, 100, 1000);
        pstmt.setLong(1, employeeId);
        pstmt.setDate(2, month);
        pstmt.setString(3, "Tax Deduction");
        pstmt.setString(4, "Monthly Tax");
        pstmt.setString(5, "PERCENTAGE");
        pstmt.setDouble(6, deductionAmount);
        pstmt.addBatch();
        return deductionAmount;
    }

    private static double insertTaxes(PreparedStatement pstmt, long employeeId, Date month, double grossSalary, String taxName) throws SQLException {
        double taxRate = 0.14;
        double taxAmount = grossSalary * taxRate;
        pstmt.setLong(1, employeeId);
        pstmt.setDate(2, month);
        pstmt.setDouble(3, grossSalary);
        pstmt.setString(4, taxName);
        pstmt.setDouble(5, taxRate);
        pstmt.setString(6, "PERCENTAGE");
        pstmt.setDouble(7, taxAmount);
        pstmt.addBatch();

        String insertTax = "INSERT INTO taxes (employee_id, month, gross_salary, tax_name, tax_rate, tax_type, tax_amount) VALUES (?,?,?,?,?,?,?)";

        return taxAmount;
    }

    private static void insertSalaryDetails(PreparedStatement pstmt, long employeeId, Date month, double basicSalary, double totalAllowances, double totalDeductions, double grossSalary, double totalTaxes) throws SQLException {
        double netSalary = grossSalary - totalTaxes;
        pstmt.setLong(1, employeeId);
        pstmt.setDate(2, month);
        pstmt.setDouble(3, basicSalary);
        pstmt.setDouble(4, totalAllowances);
        pstmt.setDouble(5, totalDeductions);
        pstmt.setDouble(6, grossSalary);
        pstmt.setDouble(7, totalTaxes);
        pstmt.setDouble(8, netSalary);
        pstmt.addBatch();
    }

    private static void insertBankDetails(PreparedStatement pstmt, long employeeId) throws SQLException {
        pstmt.setLong(1, employeeId);
        pstmt.setString(2, faker.finance().iban());
        pstmt.setString(3, faker.finance().bic());
        pstmt.setString(4, faker.number().digits(4));
        pstmt.addBatch();
    }

    private static java.sql.Date convertUtilToSqlDate(java.util.Date date) {
        return new java.sql.Date(date.getTime());
    }
}
