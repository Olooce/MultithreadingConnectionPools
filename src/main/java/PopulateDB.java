import com.github.javafaker.Faker;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class PopulateDB {
    private static final int NUM_THREADS = 6;
    private static final int MAX = 5000;
    private static final int MIN = 1000;
    private static final long TARGET_RECORDS = 10_000_000;
    private static final AtomicLong records = new AtomicLong();
    private static final AtomicLong populated_records = new AtomicLong();

    private static final Connection[] connections = new Connection[NUM_THREADS * 2];

    static {
        for (int i = 0; i < connections.length; i++) {
            connections[i] = ConnectDB.getConnection();
        }
    }


    public static void main(String[] args) throws InterruptedException {

            Random random = new Random();
            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
            int NUM_RECORDS_FOR_THREAD;

            try {
                while(records.get() < TARGET_RECORDS) {
                    NUM_RECORDS_FOR_THREAD = Math.min(random.nextInt(MAX - MIN + 1) + MIN, (int) (TARGET_RECORDS - records.get()));
                    if(NUM_RECORDS_FOR_THREAD <(TARGET_RECORDS - records.get())){
                        NUM_RECORDS_FOR_THREAD = (int) (TARGET_RECORDS - records.get());
                    }
                    executor.execute(new InsertTask(NUM_RECORDS_FOR_THREAD, populated_records));
                    records.getAndAdd(NUM_RECORDS_FOR_THREAD);
                    //TimeUnit.MILLISECONDS.sleep(100);
                }
            } finally{
            executor.shutdown();

            try {
                if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
        }

        try {
            for (Connection conn : connections) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    // Runnable task for inserting records
    private static class InsertTask implements Runnable {
        private final Faker faker;
        private final int NUM_RECORDS_FOR_THREAD;
        private final AtomicLong populated_records;

        public InsertTask(int NUM_RECORD_FOR_THREAD, AtomicLong records) {
            this.faker = new Faker();
            this.NUM_RECORDS_FOR_THREAD = NUM_RECORD_FOR_THREAD;
            this.populated_records = records;
        }


        @Override
        public void run() {
            Connection conn = null;for (int i= 0; i <3; i++) {
                try {
                    int l = ((int) (Math.random() * connections.length));
                    conn = connections[l];

                    if (conn == null || conn.isClosed()){
                        conn   = ConnectDB.getConnection();
                        break;
                    }
                    else if (!conn.isClosed()){
                        break;
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                if (conn != null && !conn.isClosed()){
                    conn.setAutoCommit(false);

                    String insertEmp = "INSERT INTO employees (name, dob, gender, department_id, employment_type, employment_date) VALUES (?, ?, ?, ?, ?, ?)";
                    String insertCon = "INSERT INTO contact_info (employee_id, address, phone_no, email, emergency_contact_no, emergency_contact_name) VALUES (?, ?, ?, ?, ?, ?)";
                    String insertSal = "INSERT INTO salaries (employee_id, month, basic_salary, total_allowances, total_deductions, total_gross_earnings, tax_relief, tax_relief_description, total_taxes, net_salary) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    String insertAllowance = "INSERT INTO allowances (employee_id, month, allowance_name, allowance_description,allowance_rate, allowance_type, allowance_amount) VALUES (?, ?, ?, ?, ?, ?, ?)";
                    String insertDeduction = "INSERT INTO deductions (employee_id, month, deduction_name, deduction_description, deduction_type, deduction_amount) VALUES ( ?, ?, ?, ?, ?, ?)";
                    String insertTax = "INSERT INTO taxes (employee_id, month, gross_salary, tax_name, tax_rate, tax_type,tax_amount) VALUES (?, ?, ?, ?, ?, ?, ?)";
                    String insertBank = "INSERT INTO bank_details (employee_id, account_no, bank_name, branch_code) VALUES (?, ?, ?, ?)";

                    for (int i = 0; i < NUM_RECORDS_FOR_THREAD; i++) {
                        long employeeId = insertEmployee(conn, insertEmp);

                        insertContactInfo(conn, insertCon, employeeId);
                        double basicSalary = faker.number().randomDouble(2, 50000, 2000000);
                        for (int j = 0; j < 3; j++) {
                            Date month = convertUtilToSqlDate(new Date(faker.date().past(365, TimeUnit.DAYS).getTime()));
                            double total_allowances = 0.0;
                            total_allowances += insertAllowances(conn, insertAllowance, employeeId, month, basicSalary,0.03, "House Allowance");
                            total_allowances += insertAllowances(conn, insertAllowance, employeeId, month, basicSalary,0.015, "Transport Allowance");
                            total_allowances += insertAllowances(conn, insertAllowance, employeeId, month, basicSalary, 0.02,"Mortgage Allowance");
                            double total_deductions = insertDeductions(conn, insertDeduction, employeeId, month);
                            double grossSalary = basicSalary + total_allowances - total_deductions;
                            double total_taxes = insertTaxes(conn, insertTax, employeeId, month, grossSalary, "PAYE");

                            insertSalaryDetails(conn, insertSal, employeeId, month, basicSalary, total_allowances, total_deductions, grossSalary, total_taxes);
                            basicSalary = basicSalary * 1.02;
                        }

                        insertBankDetails(conn, insertBank, employeeId);

                        populated_records.incrementAndGet();

                        if(populated_records.get() % 1000 == 0){
                            System.out.println("Populated " + populated_records.get() + " records");
                            conn.commit();
                        }
                    }

                    conn.commit();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }

        private java.sql.Date convertUtilToSqlDate(Date utilDate) {
            return new java.sql.Date(utilDate.getTime());
        }

        private long insertEmployee(Connection conn, String sql) throws SQLException {
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

        private void insertContactInfo(Connection conn, String sql, long employeeId) throws SQLException {
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

        private void insertSalaryDetails(Connection conn, String sql, long employeeId, Date month, double basicSalary, double totalAllowances, double totalDeductions, double grossSalary, double totalTaxes) throws SQLException {
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

        private double insertAllowances(Connection conn, String sql, long employeeId, Date month, double basicSalary,double rate, String allowanceName) throws SQLException {
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setLong(1, employeeId);
                pstmt.setDate(2, month);
                pstmt.setString(3, allowanceName);
                pstmt.setString(4, faker.lorem().sentence());
                pstmt.setDouble(5,rate);
                pstmt.setString(6, "PERCENTAGE");
                double allowanceAmount = basicSalary * rate;
                pstmt.setDouble(7, allowanceAmount);

                pstmt.executeUpdate();
                return allowanceAmount;
            }
        }

        private double insertDeductions(Connection conn, String sql, long employeeId, Date month) throws SQLException {
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


        private double insertTaxes(Connection conn, String sql, long employeeId, Date month, double grossSalary, String taxName) throws SQLException {
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setLong(1, employeeId);
                pstmt.setDate(2, month);
                pstmt.setDouble(3, grossSalary); // Assuming a constant gross salary
                pstmt.setString(4, taxName);
                pstmt.setDouble(5, 0.14); // Assuming a 15% tax rate
                pstmt.setString(6, "PERCENTAGE");

                double taxAmount = grossSalary * 0.14;
                pstmt.setDouble(7, taxAmount);
                pstmt.executeUpdate();
                return taxAmount;
            }
        }

        private void insertBankDetails(Connection conn, String sql, long employeeId) throws SQLException {
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setLong(1, employeeId);
                pstmt.setString(2, faker.finance().iban());
                pstmt.setString(3, faker.company().name());
                pstmt.setString(4, faker.finance().bic());

                pstmt.executeUpdate();
            }
        }
    }
}
