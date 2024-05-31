import java.sql.*;

public class Calc_Bonus_by_stmt_4 {
    public static void main(String[] args) {
        String url = "jdbc:oracle:thin:@192.168.217.206:1521/KOPODA";
        String user = "DA2411";
        String password = "da11";
        String yyyymm = "202406";
        String sql;
        ResultSet rs = null;
        Connection conn = null;
        Statement stmt = null;
        Statement stmt_ins = null;

        try {
            long startTime = System.currentTimeMillis();
            // JDBC 드라이버 로딩
            Class.forName("oracle.jdbc.driver.OracleDriver");

            // db 연결 설정
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
            stmt_ins = conn.createStatement();

            conn.setAutoCommit(false);  // 자동 커밋 비활성화

            // 고객 정보를 조회하는 SQL 쿼리
            sql = "SELECT id, email, credit_limit, address1, enroll_dt, gender FROM customer WHERE enroll_dt >= TO_DATE('2018-01-01', 'YYYY-MM-DD')";
            // sql 실행 (전송)
            rs = stmt.executeQuery(sql);

            // 각 고객 정보에 대해 보너스 쿠폰을 계산하고 삽입
            int count = 0;
            while (rs.next()) {
                String customerId = rs.getString("id");
                String email = rs.getString("email");
                int creditLimit = rs.getInt("credit_limit");
                String address = rs.getString("address1");
                Date enrollDt = rs.getDate("enroll_dt");
                String gender = rs.getString("gender");
                String coupon = "";


                // 보너스 쿠폰 계산 로직
                if (creditLimit < 1000) {
                    coupon = "AA";
                } else if (creditLimit >= 1000 && creditLimit <= 2999) {
                    coupon = "BB";
                } else if (creditLimit >= 3000 && creditLimit <= 3999) {
                    if (address.contains("서울 송파구 풍납1동") && gender.equalsIgnoreCase("F")) {
                        coupon = "C2";
                    } else {
                        coupon = "CC";
                    }
                } else {
                    coupon = "DD";
                }

                // 보너스 쿠폰 삽입을 위한 SQL 쿼리
                sql = String.format("INSERT INTO bonus_coupon (yyyymm, customer_id, email, coupon, credit_point, send_dt, receive_dt, use_dt) " +
                                "VALUES ('%s', '%s', '%s', '%s', %d, SYSDATE, NULL, NULL)",
                        yyyymm, customerId, email, coupon, creditLimit);

                // 보너스 쿠폰 삽입 쿼리 실행
                stmt_ins.executeUpdate(sql);
                count++; //횟수 세는 변수 설정

                //10000건 단위로 커밋
                if (count % 10000 == 0) {
                    conn.commit(); //트랜잭션 커밋
                    System.out.println("10000개 넣고 커밋");
                    count = 0;
                }
            }

            if (count % 10000 != 0) {
                conn.commit();
                System.out.println("남은 건 커밋");
            }

            // 경과 시간 출력
            long endTime = System.currentTimeMillis();
            System.out.println("경과 시간: " + (endTime - startTime) + "ms");

        }  catch (ClassNotFoundException e) {
            // JDBC 드라이버 클래스를 찾을 수 없음
            System.err.println("JDBC 드라이버 클래스를 찾을 수 없습니다: " + e.getMessage());
            e.printStackTrace();
            try {
                if (conn != null) {
                    // 트랜잭션 롤백
                    conn.rollback();
                }
            } catch (SQLException rollbackEx) {
                // 롤백 중 예외 발생
                System.err.println("트랜잭션 롤백 중 예외가 발생했습니다: " + rollbackEx.getMessage());
                rollbackEx.printStackTrace();
            }
        } catch (SQLException e) {
            // SQL 실행 중 예외 발생
            System.err.println("SQL 예외가 발생했습니다: " + e.getMessage());
            e.printStackTrace();
            try {
                if (conn != null) {
                    // 트랜잭션 롤백
                    conn.rollback();
                }
            } catch (SQLException rollbackEx) {
                // 롤백 중 예외 발생
                System.err.println("트랜잭션 롤백 중 예외가 발생했습니다: " + rollbackEx.getMessage());
                rollbackEx.printStackTrace();
            }
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                // 자원 해제 중 예외 발생
                System.err.println("자원 해제 중 예외가 발생했습니다: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}