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
    private static final int MAX = 500;
    private static final int MIN = 100;
    static final long TARGET_RECORDS = 1000000;
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
                numRecords = 500; //Math.min(random.nextInt(MAX - MIN + 1) + MIN, (int) (TARGET_RECORDS - records.get()));
                queue.put(numRecords);
                records.getAndAdd(numRecords);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            TimeUnit.MICROSECONDS.sleep(50);
            executor.execute(new Worker(queue));
        }
        new Thread(() -> {
            while (!executor.isTerminated()) {
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

        while (true) {
            try {
                TimeUnit.MICROSECONDS.sleep(10);
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

                String insertEmp = "INSERT INTO employees (name, dob, gender, department_id, employment_type, employment_date) VALUES (?, ?, ?, ?, ?, ?)";
                String insertCon = "INSERT INTO contact_info (employee_id, address, phone_no, email, emergency_contact_no, emergency_contact_name) VALUES (?, ?, ?, ?, ?, ?)";
                String insertSal = "INSERT INTO salaries (employee_id, month, basic_salary, total_allowances, total_deductions, total_gross_earnings, tax_relief, tax_relief_description, total_taxes, net_salary) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                String insertAllowance = "INSERT INTO allowances (employee_id, month, allowance_name, allowance_description, allowance_rate, allowance_type, allowance_amount) VALUES (?, ?, ?, ?, ?, ?, ?)";
                String insertDeduction = "INSERT INTO deductions (employee_id, month, deduction_name, deduction_description, deduction_type, deduction_amount) VALUES (?, ?, ?, ?, ?, ?)";
                String insertTax = "INSERT INTO taxes (employee_id, month, gross_salary, tax_name, tax_rate, tax_type, tax_amount) VALUES (?, ?, ?, ?, ?, ?, ?)";
                String insertBank = "INSERT INTO bank_details (employee_id, account_no, bank_name, branch_code) VALUES (?, ?, ?, ?)";

                for (int i = 0; i < numRecordsForThread; i++) {
                    long employeeId = insertEmployee(conn, insertEmp);

                    insertContactInfo(conn, insertCon, employeeId);

                    double basicSalary = random.nextDouble() * (2000000 - 50000) + 50000;
                    for (int j = 0; j < 3; j++) {
                        Date month = convertUtilToSqlDate(new java.util.Date(faker.date().past(365, TimeUnit.DAYS).getTime()));
                        double totalAllowances = 0.0;
                        totalAllowances += insertAllowances(conn, insertAllowance, employeeId, month, basicSalary, 0.03, "House Allowance");
                        totalAllowances += insertAllowances(conn, insertAllowance, employeeId, month, basicSalary, 0.015, "Transport Allowance");
                        totalAllowances += insertAllowances(conn, insertAllowance, employeeId, month, basicSalary, 0.02, "Mortgage Allowance");
                        double totalDeductions = insertDeductions(conn, insertDeduction, employeeId, month);
                        double grossSalary = basicSalary + totalAllowances - totalDeductions;
                        double totalTaxes = insertTaxes(conn, insertTax, employeeId, month, grossSalary, "PAYE");

                        insertSalaryDetails(conn, insertSal, employeeId, month, basicSalary, totalAllowances, totalDeductions, grossSalary, totalTaxes);
                        basicSalary = basicSalary * 1.02;
                    }

                    insertBankDetails(conn, insertBank, employeeId);
                    MultiThreadedDBInserter.populatedRecords.incrementAndGet();
                }
                conn.commit();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            MultiThreadedDBInserter.activeThreadCount.decrementAndGet();
        };
    }

    private static java.sql.Date convertUtilToSqlDate(java.util.Date utilDate) {
        return new java.sql.Date(utilDate.getTime());
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

    private static void insertContactInfo(Connection conn, String sql, long employeeId) throws SQLException {
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

    private static void insertSalaryDetails(Connection conn, String sql, long employeeId, Date month, double basicSalary, double totalAllowances, double totalDeductions, double grossSalary, double totalTaxes) throws SQLException {
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

    private static double insertAllowances(Connection conn, String sql, long employeeId, Date month, double basicSalary, double rate, String allowanceName) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, employeeId);
            pstmt.setDate(2, month);
            pstmt.setString(3, allowanceName);
            pstmt.setString(4, faker.lorem().sentence());
            pstmt.setDouble(5, rate);
            pstmt.setString(6, "PERCENTAGE");
            double allowanceAmount = basicSalary * rate;
            pstmt.setDouble(7, allowanceAmount);

            pstmt.executeUpdate();
            return allowanceAmount;
        }
    }

    private static double insertDeductions(Connection conn, String sql, long employeeId, Date month) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, employeeId);
            pstmt.setDate(2, month);
            pstmt.setString(3, "HEALTH INSURANCE"); // Deduction name
            pstmt.setString(4, faker.lorem().sentence()); // Deduction description
            pstmt.setString(5, "FIXED"); // Deduction type
            double deductionAmount = faker.number().randomDouble(2, 500, 2000); // Deduction amount
            pstmt.setDouble(6, deductionAmount);

            pstmt.executeUpdate();
            return deductionAmount;
        }
    }

    private static double insertTaxes(Connection conn, String sql, long employeeId, Date month, double grossSalary, String taxName) throws SQLException {
        try (PreparedStatement ppst = conn.prepareStatement(sql)) {
            ppst.setLong(1, employeeId);
            ppst.setDate(2, month);
            ppst.setDouble(3, grossSalary); // Assuming a constant gross salary
            ppst.setString(4, taxName);
            ppst.setDouble(5, 0.14); // Assuming a 14% tax rate
            ppst.setString(6, "PERCENTAGE");

            double taxAmount = grossSalary * 0.14;
            ppst.setDouble(7, taxAmount);
            ppst.executeUpdate();
            return taxAmount;
        }
    }

    private static void insertBankDetails(Connection conn, String sql, long employeeId) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, employeeId);
            pstmt.setString(2, faker.finance().iban());
            pstmt.setString(3, faker.company().name());
            pstmt.setString(4, faker.finance().bic());

            pstmt.executeUpdate();
        }
    }

}


