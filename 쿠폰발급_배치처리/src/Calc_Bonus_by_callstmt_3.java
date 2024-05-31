import java.sql.*;

public class Calc_Bonus_by_callstmt_3 {
    public static void main(String[] args) {
        String url = "jdbc:oracle:thin:@192.168.217.206:1521/KOPODA";
        String user = "DA2411";
        String password = "da11";
        Connection conn = null;
        CallableStatement cstmt = null;
        String yyyymm = "202406";

        String plSql =
                "begin " +
                "INSERT INTO bonus_coupon (yyyymm, customer_id, email, coupon, credit_point, send_dt, receive_dt, use_dt) " +
                "SELECT ?, id, email, " +
                "    CASE " +
                "        WHEN credit_limit < 1000 THEN 'AA' " +
                "        WHEN credit_limit >= 1000 AND credit_limit <= 2999 THEN 'BB' " +
                "        WHEN credit_limit >= 3000 AND credit_limit <= 3999 THEN " +
                "            CASE " +
                "                WHEN address1 LIKE '%송파구 풍납1동%' AND gender = 'F' THEN 'C2' " +
                "                ELSE 'CC' " +
                "            END " +
                "        ELSE 'DD' " +
                "    END AS coupon, " +
                "    credit_limit, " +
                "    SYSDATE, " +
                "    NULL, " +
                "    NULL " +
                "FROM customer " +
                "WHERE enroll_dt >= TO_DATE('2018-01-01', 'YYYY-MM-DD');" +
                "commit; "+
                "end;";

        try {
            long startTime = System.currentTimeMillis();
            // JDBC 드라이버 로딩
            Class.forName("oracle.jdbc.driver.OracleDriver");

            //db 연결 설정
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false); //자동 커밋 비활성화

            // CallableStatement 준비
            cstmt = conn.prepareCall(plSql);
            cstmt.setString(1,yyyymm);// yyyymm 파라미터 설정
            // PL/SQL 블록 실행
            cstmt.execute();

            long endTime = System.currentTimeMillis();// 종료 시간 측정
            System.out.println("경과 시간: " + (endTime - startTime) + "ms");

        } catch (ClassNotFoundException e) {
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
            //자원 해제
            try {
                if (cstmt != null) {
                    cstmt.close();
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
