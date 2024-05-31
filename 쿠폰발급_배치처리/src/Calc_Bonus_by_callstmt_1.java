import java.sql.*;

public class Calc_Bonus_by_callstmt_1 {
    public static void main(String[] args) {
        String url = "jdbc:oracle:thin:@192.168.217.206:1521/KOPODA";
        String user = "DA2411";
        String password = "da11";
        Connection conn = null;
        CallableStatement cstmt = null;
        String yyyymm = "202406";

        // PL/SQL 블록
        String plSql = "DECLARE " +
                // 커서 정의: 조건에 맞는 고객 데이터를 선택
                "    CURSOR bonus_cur IS " +
                "        SELECT id, email, credit_limit, address1, enroll_dt, gender " +
                "        FROM customer " +
                "        WHERE enroll_dt >= TO_DATE('2018-01-01', 'YYYY-MM-DD'); " +
                "    " +
                "    v_id customer.id%TYPE; " +
                "    v_email customer.email%TYPE; " +
                "    v_credit_limit customer.credit_limit%TYPE; " +
                "    v_address customer.address1%TYPE; " +
                "    v_enroll_dt customer.enroll_dt%TYPE; " +
                "    v_gender customer.gender%TYPE; " +
                "    v_coupon bonus_coupon.coupon%TYPE; " +
                // 처리한 레코드 수를 세기 위한 변수
                "    v_count NUMBER := 0; " +
                "BEGIN " +
                "    OPEN bonus_cur; " + // 커서 열기
                "    LOOP " +
                "        FETCH bonus_cur INTO v_id, v_email, v_credit_limit, v_address, v_enroll_dt, v_gender; " + //1건씩 FETCH
                "        EXIT WHEN bonus_cur%NOTFOUND; " +
                "        IF v_credit_limit < 1000 THEN " + //PL/SQL 에서 보너스 계산
                "            v_coupon := 'AA'; " +
                "        ELSIF v_credit_limit >= 1000 AND v_credit_limit <= 2999 THEN " +
                "            v_coupon := 'BB'; " +
                "        ELSIF v_credit_limit >= 3000 AND v_credit_limit <= 3999 THEN " +
                "            IF v_address LIKE '%송파구 풍납1동%' AND v_gender = 'F' THEN " +
                "                v_coupon := 'C2'; " +
                "            ELSE " +
                "                v_coupon := 'CC'; " +
                "            END IF; " +
                "        ELSE " +
                "            v_coupon := 'DD'; " +
                "        END IF; " +
                "        INSERT INTO bonus_coupon(yyyymm, customer_id, email, coupon, credit_point, send_dt, receive_dt, use_dt) " +
                "        VALUES (?, v_id, v_email, v_coupon, v_credit_limit, SYSDATE, NULL, NULL); " +
                "        v_count := v_count + 1; " +
                "        IF v_count >= 10000 THEN " +
                "            COMMIT; " +
                "            v_count := 0; " +
                "        END IF; " +
                "    END LOOP; " +
                "    CLOSE bonus_cur; " +
                "    COMMIT; " +
                "END;";

        try {
            long startTime = System.currentTimeMillis(); // 시작 시간 측정
            // JDBC 드라이버 로딩
            Class.forName("oracle.jdbc.driver.OracleDriver");

            // 데이터베이스 연결 설정
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false); // 자동 커밋 비활성화

            // CallableStatement 준비
            cstmt = conn.prepareCall(plSql);
            cstmt.setString(1, yyyymm); // yyyymm 파라미터 설정

            // PL/SQL 블록 실행
            cstmt.execute();

            long endTime = System.currentTimeMillis(); // 종료 시간 측정
            System.out.println("경과 시간: " + (endTime - startTime) + "ms");

        } catch (ClassNotFoundException e) {
            // JDBC 드라이버를 찾을 수 없는 경우
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
            // 자원 해제
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