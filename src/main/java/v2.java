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

public class v2 {
    static final AtomicLong TARGET_RECORDS = new AtomicLong(5_0_000);
    static final AtomicInteger NUM_THREADS = new AtomicInteger(50);
    private static final int MAX = 9000;
    private static final int MIN = 3000;
    private static final AtomicLong records = new AtomicLong();
    static final AtomicLong populatedRecords = new AtomicLong();
    static final AtomicLong createdRecords = new AtomicLong();
    static final AtomicInteger activeThreadCount = new AtomicInteger();
    static final AtomicLong activeInserts = new AtomicLong();
    static final AtomicInteger activeThreads = new AtomicInteger();
    private static final ThreadPoolExecutor dataPool = new ThreadPoolExecutor(NUM_THREADS.get() , NUM_THREADS.get() ,
            60L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());
    private static final ThreadPoolExecutor executePool = new ThreadPoolExecutor(NUM_THREADS.get() , NUM_THREADS.get() ,
            60L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());

    public static void main(String[] args) {
        BlockingQueue<Integer> queue = new LinkedBlockingQueue<>();
        BlockingQueue<PreparedStatement[]> statementQueue = new LinkedBlockingQueue<>();
        Random random = new Random();
        int numRecords;

        try {
            while (records.get() < TARGET_RECORDS.get()) {
                numRecords = Math.min(random.nextInt(MAX - MIN + 1) + MIN, (int) (TARGET_RECORDS.get() - records.get()));
                queue.put(numRecords);
                records.getAndAdd(numRecords);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }



        new Thread(() -> {
            while (populatedRecords.get() < TARGET_RECORDS.get()) {

                if (!statementQueue.isEmpty()  && (activeInserts.get() < (NUM_THREADS.get() - activeThreadCount.get()))){
                    executePool.execute(new StatementExecutor(statementQueue));
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt(); // Restore the interrupted status
                }
                if((!queue.isEmpty() && createdRecords.get() < TARGET_RECORDS.get() ) && (activeThreadCount.get() <NUM_THREADS.get()/2 ) || ((activeInserts.get() < NUM_THREADS.get() - activeThreadCount.get()) && statementQueue.isEmpty())){
                    dataPool.execute(new DataGenerator(statementQueue, queue));
                }

                try {
                    Thread.sleep(4500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt(); // Restore the interrupted status
                }

//                 if(activeThreads.get() < NUM_THREADS.get()) {
//                     if (activeThreads.get() < NUM_THREADS.get() && (!queue.isEmpty() && (createdRecords.get() < TARGET_RECORDS.get()
//                             && (activeThreads.get() < NUM_THREADS.get() && statementQueue.isEmpty())))) {
//                         activeThreadCount.incrementAndGet();
//                         activeThreads.incrementAndGet();
//                         dataPool.execute(() -> {
//                             try {
//                                 dataPool.execute(new DataGenerator(statementQueue, queue));
//                             } finally {
//                                 activeThreadCount.decrementAndGet();
//                                 activeThreads.decrementAndGet();
//                             }
//                         });
//                     }
//
//                     if (!statementQueue.isEmpty()
//                             && (activeThreads.get() < NUM_THREADS.get())) {
//                         activeInserts.incrementAndGet();
//                         activeThreads.incrementAndGet();
//                         executePool.execute(() -> {
//                             try {
//                                 executePool.execute(new StatementExecutor(statementQueue));
//                             } finally {
//                                 activeInserts.decrementAndGet();
//                                 activeThreads.decrementAndGet();
//                             }
//                         });
//                     }
//                 }
            }
        }).start();



        new Thread(() -> {
            long startTime = System.currentTimeMillis();
            while (populatedRecords.get() < TARGET_RECORDS.get()) {
                long elapsedTime = System.currentTimeMillis() - startTime;

                System.out.println("\nElapsed time: " + elapsedTime / 1000 + " seconds");
                System.out.println("Active Generator threads: " + activeThreadCount.get());
                System.out.println("Active Inserter threads: " + activeInserts.get());
                System.out.println("Created " + createdRecords.get() + " records");
                System.out.println("Populated " + populatedRecords.get() + " records");

                try {
                    Thread.sleep(5000); // Sleep for 30 seconds
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt(); // Restore the interrupted status
                }
            }
        }).start();

        //executePool.shutdown();
        //dataPool.shutdown();

        try {
            if (!executePool.awaitTermination(1, TimeUnit.DAYS)) {
                executePool.shutdownNow();
            }
        } catch (InterruptedException e) {
            executePool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        try {
            if (!dataPool.awaitTermination(1, TimeUnit.DAYS)) {
                dataPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            dataPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.println("\nSuccessfully Populated " + populatedRecords.get() + " records");
    }
}

class DataGenerator implements Runnable {
    private final BlockingQueue<PreparedStatement[]> statementsQueue;
    private final BlockingQueue<Integer> queue;
    private static final Random random = new Random();
    private static final Faker faker = new Faker();

    public DataGenerator(BlockingQueue<PreparedStatement[]> statementsQueue, BlockingQueue<Integer> queue) {
        this.statementsQueue = statementsQueue;
        this.queue = queue;
    }

    @Override
    public void run() {
        //int randomNumber;
        while (true) {
            /*try {
                randomNumber = random.nextInt(650);
                TimeUnit.MICROSECONDS.sleep(randomNumber);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }*/

            try {
                if (v2.createdRecords.get() >= v2.TARGET_RECORDS.get() && queue.isEmpty()) {
                    break;
                }

                Integer numRecordsForThread = queue.poll(5, TimeUnit.MICROSECONDS);
                if (numRecordsForThread != null) {
                    task(numRecordsForThread).run();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public Runnable task(int numRecordsForThread) {
        v2.activeThreadCount.incrementAndGet();
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

                List<PreparedStatement> batchQueue = new ArrayList<>();

                int batch = 0;
             try {
                 PreparedStatement pstmtAllowance = null;
                 PreparedStatement pstmtCon = null;
                 PreparedStatement pstmtSal = null;
                 PreparedStatement pstmtBank = null;
                 for (int i = 0; i < numRecordsForThread; i++) {
                     PreparedStatement pstmtEmp = conn.prepareStatement(insertEmp, PreparedStatement.RETURN_GENERATED_KEYS);
                     pstmtCon = conn.prepareStatement(insertCon);
                     pstmtSal = conn.prepareStatement(insertSal);
                     pstmtAllowance = conn.prepareStatement(insertAllowance);
                     PreparedStatement pstmtDeduction = conn.prepareStatement(insertDeduction);
                     PreparedStatement pstmtTax = conn.prepareStatement(insertTax);
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
                         basicSalary *= 1.02;
                     }

                     insertContactInfo(pstmtCon, employeeId);
                     insertBankDetails(pstmtBank, employeeId);

                     batchQueue.add(pstmtCon);
                     batchQueue.add(pstmtSal);
                     batchQueue.add(pstmtAllowance);
                     batchQueue.add(pstmtDeduction);
                     batchQueue.add(pstmtTax);
                     batchQueue.add(pstmtBank);

                     batch++;
                     if (batch % 6 == 0) {
                         conn.commit();
                         statementsQueue.put(batchQueue.toArray(new PreparedStatement[0]));
                     }
                     batchQueue.clear();


                     v2.createdRecords.incrementAndGet();
                 }
                 conn.commit();



                 if (!batchQueue.isEmpty()) {
                     statementsQueue.put(batchQueue.toArray(new PreparedStatement[0]));
                 }
                 conn.commit();
                 assert pstmtCon != null;
                 pstmtAllowance.close();
                 pstmtCon.close();
                 pstmtSal.close();
                 pstmtBank.close();

             } catch (SQLException | InterruptedException e) {
                 throw new RuntimeException(e);
             }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                v2.activeThreadCount.decrementAndGet();
            }
        };
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
        pstmt.setDouble(7, 0);
        pstmt.setString(8, "");
        pstmt.setDouble(9, totalTaxes);
        pstmt.setDouble(10, grossSalary - totalTaxes);
        pstmt.addBatch();
    }

    private static void insertBankDetails(PreparedStatement pstmt, long employeeId) throws SQLException {
        pstmt.setLong(1, employeeId);
        pstmt.setString(2, faker.finance().iban());
        pstmt.setString(3, faker.company().name());
        pstmt.setString(4, faker.address().zipCode());
        pstmt.addBatch();
    }

    private static Date convertUtilToSqlDate(java.util.Date date) {
        return new java.sql.Date(date.getTime());
    }
}

//class StatementExecutor implements Runnable {
//    private final BlockingQueue<PreparedStatement[]> queue;
//
//    public StatementExecutor(BlockingQueue<PreparedStatement[]> queue) {
//        this.queue = queue;
//    }
//
//    @Override
//    public void run() {
//        while (true) {
//            v2.activeInserts.incrementAndGet();
//            try {
//                PreparedStatement[] statements = queue.poll(5, TimeUnit.MICROSECONDS);
//                if(statements !=null) {
//                    executeAndCommitBatches(statements);
//                }
//            } catch (InterruptedException | SQLException e) {
//                throw new RuntimeException(e);
//            }
//            v2.activeInserts.decrementAndGet();
//        }
//    }
//
//    private static void executeAndCommitBatches(PreparedStatement[] statements) throws SQLException {
//        if(statements != null) {
//            try (Connection conn = Data_Source.getConnection()) {
//                conn.setAutoCommit(false);
//                int count = 0;
//                for (PreparedStatement pstmt : statements) {
//                    if (pstmt != null) {
//                        pstmt.executeBatch();
//                        count++;
//                    }
//                }
//                v2.populatedRecords.getAndAdd(count / 6);
//                conn.commit();
//            }
//        }
//        else{
//            System.out.println("Called null batch.");
//        }
//    }
//}


class StatementExecutor implements Runnable {
    private final BlockingQueue<PreparedStatement[]> queue;

    public StatementExecutor(BlockingQueue<PreparedStatement[]> queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        while (true) {
            v2.activeInserts.incrementAndGet();
            try {
                PreparedStatement[] statements = new PreparedStatement[3000];
                int count = 0;
                for (int i = 0; i < 3000; i++) {
                    PreparedStatement[] partialStatements = queue.poll(5, TimeUnit.MICROSECONDS);
                    if (partialStatements == null) {
                        break;
                    }
                    if (count + partialStatements.length > statements.length) {
                        break;
                    }
                    System.arraycopy(partialStatements, 0, statements, count, partialStatements.length);
                    count += partialStatements.length;
                }
                if (count > 0) {
                    executeAndCommitBatches(statements, count);
                }
            } catch (InterruptedException | SQLException e) {
                throw new RuntimeException(e);
            }
            v2.activeInserts.decrementAndGet();
        }
    }

    private static void executeAndCommitBatches(PreparedStatement[] statements, int count) throws SQLException {
        try (Connection conn = Data_Source.getConnection()) {
            conn.setAutoCommit(false);
            int batchCount = 0;
            for (int i = 0; i < count; i++) {
                PreparedStatement pstmt = statements[i];
                if (pstmt!= null) {
                    pstmt.executeBatch();
                    batchCount++;
                }
            }
            v2.populatedRecords.getAndAdd(batchCount / 6);
            conn.commit();
        } catch (SQLException e) {
            /*try {
                // Roll back the transaction if an exception occurs
                Data_Source.getConnection().rollback();
            } catch (SQLException ex) {
                // Log the rollback exception
                ex.printStackTrace();
            }*/
            throw e;
        }
    }
}