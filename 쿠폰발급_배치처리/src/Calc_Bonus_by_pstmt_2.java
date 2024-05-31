import java.sql.*;

public class Calc_Bonus_by_pstmt_2 {
    public static void main(String[] args) {
        // 데이터베이스 연결 정보
        String url = "jdbc:oracle:thin:@192.168.217.206:1521/KOPODA";
        String user = "DA2411";
        String password = "da11";
        String yyyymm = "202406";

        // JDBC 객체들
        ResultSet rs = null;
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;

        // SQL 쿼리
        String selectSql = "SELECT id, email, credit_limit, address1, enroll_dt, gender FROM customer WHERE enroll_dt >= TO_DATE('2018-01-01', 'YYYY-MM-DD')";
        String insertSql = "INSERT INTO bonus_coupon(yyyymm, customer_id, email, coupon, credit_point, send_dt, receive_dt, use_dt) VALUES (?, ?, ?, ?, ?, SYSDATE, NULL, NULL)";

        try {
            long startTime = System.currentTimeMillis(); //시작 시간 측정

            // JDBC 드라이버 로딩
            Class.forName("oracle.jdbc.driver.OracleDriver");

            //db 연결 설정
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
            pstmt = conn.prepareStatement(insertSql);

            //자동 커밋 비활성화
            conn.setAutoCommit(false);

            // fetch size 설정
            stmt.setFetchSize(1000);

            // 고객 데이터 조회 (fetch)
            rs = stmt.executeQuery(selectSql);

            int count = 0; // 삽입된 레코드 수를 세기 위한 변수
            while (rs.next()) {
                String customerId = rs.getString("id");
                String email = rs.getString("email");
                int creditLimit = rs.getInt("credit_limit");
                String address = rs.getString("address1");
                Date enrollDt = rs.getDate("enroll_dt");
                String gender = rs.getString("gender");
                String coupon = "";

                // 쿠폰 조건 설정
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

                // PreparedStatement에 값 설정
                pstmt.setString(1, yyyymm);
                pstmt.setString(2, customerId);
                pstmt.setString(3, email);
                pstmt.setString(4, coupon);
                pstmt.setInt(5, creditLimit);

                // SQL 실행
                pstmt.addBatch();
                count++;

                // 10000개마다 커밋
                if (count % 1000 == 0) {
                    pstmt.executeBatch();
                    conn.commit();
                    System.out.println("1000개 넣고 커밋");
                }
            }

            // 마지막 남은 batch 실행 및 커밋
            if (count % 1000 != 0) {
                pstmt.executeBatch();
                conn.commit();
                System.out.println("남은 건 커밋");
            }

            long endTime = System.currentTimeMillis(); // 종료 시간 측정
            System.out.println("경과 시간: " + (endTime - startTime) + "ms");

        } catch (ClassNotFoundException e) {
            // JDBC 드라이버 클래스를 찾을 수 없는 경우
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
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException e) {
                // 자원 해제 중 예외 발생
                System.err.println("자원 해제 중 예외가 발생했습니다: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}

