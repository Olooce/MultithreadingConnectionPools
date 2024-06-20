import com.github.javafaker.Faker;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MultiThreadedDBInserter {
    private static final int NUM_THREADS = 200;
    private static final int MAX = 7500;
    private static final int MIN = 5000;
    static final long TARGET_RECORDS = 10_000_000;
    static final AtomicLong records = new AtomicLong();
    static final AtomicLong populatedRecords = new AtomicLong();
    static final AtomicLong createdRecords =new AtomicLong();
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
        AtomicInteger aprox = new AtomicInteger();
        new Thread(() -> {
            while (populatedRecords.get()<TARGET_RECORDS) {
                System.out.println("Active threads: " + activeThreadCount.get());
                System.out.println("Created " + MultiThreadedDBInserter.createdRecords.get() + " records");
                System.out.println("Populated " + MultiThreadedDBInserter.populatedRecords.get() + " records");
                System.out.println("Elapsed Time: "+ aprox.get()/3600 + "H: "+ (aprox.get()%3600)/60+"M (Aprox.)");
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                aprox.getAndAdd(60);
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
                randomNumber = random.nextInt(1000);
                TimeUnit.MICROSECONDS.sleep(randomNumber);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                if (MultiThreadedDBInserter.createdRecords.get() >= MultiThreadedDBInserter.TARGET_RECORDS && queue.isEmpty()) {
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
                List<PreparedStatement> batchQueue = new ArrayList<>();

                PreparedStatement pstmtCon = null;
                PreparedStatement pstmtSal = null;
                PreparedStatement pstmtAllowance = null;
                PreparedStatement pstmtDeduction = null;
                PreparedStatement pstmtTax = null;
                PreparedStatement pstmtBank = null;

                for (int i = 0; i <= numRecordsForThread; i++) {


                    PreparedStatement pstmtEmp = conn.prepareStatement(insertEmp, PreparedStatement.RETURN_GENERATED_KEYS);
                    pstmtCon = conn.prepareStatement(insertCon);
                    pstmtSal = conn.prepareStatement(insertSal);
                    pstmtAllowance = conn.prepareStatement(insertAllowance);
                    pstmtDeduction = conn.prepareStatement(insertDeduction);
                    pstmtTax = conn.prepareStatement(insertTax);
                    pstmtBank = conn.prepareStatement(insertBank);

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


                    batchQueue.add(pstmtCon);
                    batchQueue.add(pstmtSal);
                    batchQueue.add(pstmtAllowance);
                    batchQueue.add(pstmtDeduction);
                    batchQueue.add(pstmtTax);
                    batchQueue.add(pstmtBank);

                    batchSize++;
                        /*if(batchSize != 0){
                            executeAndCommitBatches(conn, pstmtCon, pstmtSal, pstmtAllowance, pstmtDeduction, pstmtTax, pstmtBank);
                        }*/
                    if (batchSize == 50/*(faker.options().option( 5,10,50, 75, 100, 150, 200)) /*batchQueue.size() % 100 == 0*/) {
                        executeAndCommitBatches(conn, batchQueue.toArray(new PreparedStatement[0]));
                        batchQueue.clear();
                    }

                    MultiThreadedDBInserter.createdRecords.incrementAndGet();
                        /*if(i==numRecordsForThread){
                            executeAndCommitBatches(conn, pstmtCon, pstmtSal, pstmtAllowance, pstmtDeduction, pstmtTax, pstmtBank);
                        }*/


                }
                if (!batchQueue.isEmpty()) {
                    try {
                        executeAndCommitBatches(conn, batchQueue.toArray(new PreparedStatement[0]));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }

                pstmtCon.close();
                pstmtSal.close();
                pstmtAllowance.close();
                pstmtDeduction.close();
                pstmtTax.close();
                pstmtBank.close();

                executeAndCommitBatches(conn, null, null, null, null, null, null);

                MultiThreadedDBInserter.activeThreadCount.decrementAndGet();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }
    private static void executeAndCommitBatches(Connection conn, PreparedStatement... statements) throws SQLException {
        int count = 0;
        for (PreparedStatement pstmt : statements) {
            if (pstmt != null) {
                pstmt.executeBatch();
                count ++;
            }
        }
        MultiThreadedDBInserter.populatedRecords.getAndAdd(count/6);
        conn.commit();
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
        pstmt.addBatch();
        //pstmt.executeBatch();
    }

    private static double insertAllowances(PreparedStatement pstmt, long employeeId, Date month, double basicSalary, double rate, String allowanceName) throws SQLException {
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

    private static double insertDeductions(PreparedStatement pstmt, long employeeId, Date month) throws SQLException {
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

    private static double insertTaxes(PreparedStatement pstmt, long employeeId, Date month, double grossSalary, String taxName) throws SQLException {
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

    private static void insertSalaryDetails(PreparedStatement pstmt, long employeeId, Date month, double basicSalary, double totalAllowances, double totalDeductions, double grossSalary, double totalTaxes) throws SQLException {
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

    private static void insertBankDetails(PreparedStatement pstmt, long employeeId) throws SQLException {
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



