import com.github.javafaker.Faker;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MultiThreadedDBInserter {
    private static final int NUM_THREADS = 145;
    private static final int MAX = 7500;
    private static final int MIN = 5000;
    static final long TARGET_RECORDS = 3_000;
    static final AtomicLong records = new AtomicLong();
    static final AtomicLong populatedRecords = new AtomicLong();
    static final AtomicInteger activeThreadCount = new AtomicInteger();
    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(NUM_THREADS, NUM_THREADS,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());

    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<Integer> queue = new LinkedBlockingQueue<>();
        Random random = new Random();

        int numRecords;

        try {
            while (records.get() < TARGET_RECORDS) {
                numRecords = /*500;*/ Math.min(random.nextInt(MAX - MIN + 1) + MIN, (int) (TARGET_RECORDS - records.get()));
                queue.put(numRecords);
                records.getAndAdd(numRecords);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            TimeUnit.MILLISECONDS.sleep(50);
            executor.execute(new Worker(queue));
        }
        new Thread(() -> {
            while (populatedRecords.get()<TARGET_RECORDS) {
                System.out.println("Active threads: " + activeThreadCount.get());
                System.out.println("Populated " + MultiThreadedDBInserter.populatedRecords.get() + " records");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }).start();

        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.DAYS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        System.out.println("\nSuccessfully Populated " + MultiThreadedDBInserter.populatedRecords.get() + " records");


    }
}

class Worker implements Runnable {
    private final BlockingQueue<Integer> queue;

    public Worker(BlockingQueue<Integer> queue) {
        this.queue = queue;
    }

    private static final Random random = new Random();
    private static final Faker faker = new Faker();

    @Override
    public void run() {
        int randomNumber;
        while (true) {
            try {
                randomNumber = random.nextInt(2000);
                TimeUnit.MICROSECONDS.sleep(randomNumber);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                if (MultiThreadedDBInserter.populatedRecords.get() >= MultiThreadedDBInserter.TARGET_RECORDS && queue.isEmpty()) {
                    break;
                }
                Integer numRecordsForThread = queue.poll(50, TimeUnit.MICROSECONDS);
                if (numRecordsForThread != null) {
                    task(numRecordsForThread).run();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public static Runnable task(int numRecordsForThread) {
        MultiThreadedDBInserter.activeThreadCount.incrementAndGet();
        return () -> {
            try (Connection conn = Data_Source.getConnection()) {
                conn.setAutoCommit(false);

                String insertEmp = "INSERT INTO employees (name, dob, gender, department_id, employment_type, employment_date) VALUES (?,?,?,?,?,?)";
                String insertCon = "INSERT INTO contact_info (employee_id, address, phone_no, email, emergency_contact_no, emergency_contact_name) VALUES (?,?,?,?,?,?)";
                String insertSal = "INSERT INTO salaries (employee_id, month, basic_salary, total_allowances, total_deductions, total_gross_earnings, tax_relief, tax_relief_description, total_taxes, net_salary) VALUES (?,?,?,?,?,?,?,?,?,?)";
                String insertAllowance = "INSERT INTO allowances (employee_id, month, allowance_name, allowance_description, allowance_rate, allowance_type, allowance_amount) VALUES (?,?,?,?,?,?,?)";
                String insertDeduction = "INSERT INTO deductions (employee_id, month, deduction_name, deduction_description, deduction_type, deduction_amount) VALUES (?,?,?,?,?,?)";
                String insertTax = "INSERT INTO taxes (employee_id, month, gross_salary, tax_name, tax_rate, tax_type, tax_amount) VALUES (?,?,?,?,?,?,?)";
                String insertBank = "INSERT INTO bank_details (employee_id, account_no, bank_name, branch_code) VALUES (?,?,?,?)";
                int batchSize = 0;
                for (int i = 0; i < numRecordsForThread; i++) {
                    long employeeId = insertEmployee(conn, insertEmp);

                    try (PreparedStatement pstmtCon = conn.prepareStatement(insertCon);
                         PreparedStatement pstmtSal = conn.prepareStatement(insertSal);
                         PreparedStatement pstmtAllowance = conn.prepareStatement(insertAllowance);
                         PreparedStatement pstmtDeduction = conn.prepareStatement(insertDeduction);
                         PreparedStatement pstmtTax = conn.prepareStatement(insertTax);
                         PreparedStatement pstmtBank = conn.prepareStatement(insertBank)) {

                        double basicSalary = random.nextDouble() * (2000000 - 50000) + 50000;
                        for (int j = 0; j < 3; j++) {
                            Date month = convertUtilToSqlDate(new java.util.Date(faker.date().past(365, TimeUnit.DAYS).getTime()));
                            double totalAllowances = 0.0;
                            totalAllowances += insertAllowances(conn, pstmtAllowance, employeeId, month, basicSalary, 0.03, "House Allowance");
                            totalAllowances += insertAllowances(conn, pstmtAllowance, employeeId, month, basicSalary, 0.015, "Transport Allowance");
                            totalAllowances += insertAllowances(conn, pstmtAllowance, employeeId, month, basicSalary, 0.02, "Mortgage Allowance");
                            double totalDeductions = insertDeductions(conn, pstmtDeduction, employeeId, month);
                            double grossSalary = basicSalary + totalAllowances - totalDeductions;
                            double totalTaxes = insertTaxes(conn, pstmtTax, employeeId, month, grossSalary, "PAYE");

                            insertSalaryDetails(conn, pstmtSal, employeeId, month, basicSalary, totalAllowances, totalDeductions, grossSalary, totalTaxes);
                            basicSalary = basicSalary * 1.02;
                        }

                        insertContactInfo(conn, pstmtCon, employeeId);
                        insertBankDetails(conn, pstmtBank, employeeId);
                        batchSize++;
                        if(batchSize % 25 == 0){
                            pstmtCon.executeBatch();
                            pstmtSal.executeBatch();
                            pstmtAllowance.executeBatch();
                            pstmtDeduction.executeBatch();
                            pstmtTax.executeBatch();
                            pstmtBank.executeBatch();
                            conn.commit();
                        }

                        MultiThreadedDBInserter.populatedRecords.incrementAndGet();
                    }
                }
                conn.commit();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            MultiThreadedDBInserter.activeThreadCount.decrementAndGet();
        };
    }

    private static long insertEmployee(Connection conn, String sql) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
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
    }

    private static void insertContactInfo(Connection conn, PreparedStatement pstmt, long employeeId) throws SQLException {
        pstmt.setLong(1, employeeId);
        pstmt.setString(2, faker.address().fullAddress());
        pstmt.setString(3, faker.phoneNumber().cellPhone());
        pstmt.setString(4, faker.internet().emailAddress());
        pstmt.setString(5, faker.phoneNumber().cellPhone());
        pstmt.setString(6, faker.name().fullName());
        pstmt.addBatch();
        //pstmt.executeBatch();
    }

    private static double insertAllowances(Connection conn, PreparedStatement pstmt, long employeeId, Date month, double basicSalary, double rate, String allowanceName) throws SQLException {
        pstmt.setLong(1, employeeId);
        pstmt.setDate(2, month);
        pstmt.setString(3, allowanceName);
        pstmt.setString(4, "Monthly: " + allowanceName);
        pstmt.setDouble(5, rate);
        pstmt.setString(6, "PERCENTAGE");
        pstmt.setDouble(7, basicSalary * rate);
        pstmt.addBatch();
        //pstmt.executeBatch();
        return basicSalary * rate;
    }

    private static double insertDeductions(Connection conn, PreparedStatement pstmt, long employeeId, Date month) throws SQLException {
        pstmt.setLong(1, employeeId);
        pstmt.setDate(2, month);
        pstmt.setString(3, "Pension Contribution");
        pstmt.setString(4, "Monthly pension contribution");
        pstmt.setString(5, "FIXED");
        pstmt.setDouble(6, 5000.0);
        pstmt.addBatch();
        //pstmt.executeBatch();
        return 5000.0;
    }

    private static double insertTaxes(Connection conn, PreparedStatement pstmt, long employeeId, Date month, double grossSalary, String taxName) throws SQLException {
        pstmt.setLong(1, employeeId);
        pstmt.setDate(2, month);
        pstmt.setDouble(3, grossSalary); // Assuming a constant gross salary
        pstmt.setString(4, taxName);
        pstmt.setDouble(5, 0.14); // Assuming a 14% tax rate
        pstmt.setString(6, "PERCENTAGE");

        double taxAmount = grossSalary * 0.14;
        pstmt.setDouble(7, taxAmount);
        pstmt.addBatch();
        return taxAmount;
    }

    private static void insertSalaryDetails(Connection conn, PreparedStatement pstmt, long employeeId, Date month, double basicSalary, double totalAllowances, double totalDeductions, double grossSalary, double totalTaxes) throws SQLException {
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

        pstmt.addBatch();
        //pstmt.executeBatch();
    }

    private static void insertBankDetails(Connection conn, PreparedStatement pstmt, long employeeId) throws SQLException {
        pstmt.setLong(1, employeeId);
        pstmt.setString(2, faker.finance().iban());
        pstmt.setString(3, faker.company().name());
        pstmt.setString(4, faker.address().zipCode());
        pstmt.addBatch();
        //pstmt.executeBatch();
    }

    private static Date convertUtilToSqlDate(java.util.Date date) {
        return new java.sql.Date(date.getTime());
    }

}



