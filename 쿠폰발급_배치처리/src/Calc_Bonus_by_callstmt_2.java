import java.sql.*;

public class Calc_Bonus_by_callstmt_2 {
    public static void main(String[] args) {
        // 데이터베이스 연결 정보
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
                // 배열 타입 정의: 각 컬럼에 대해 배열 타입 선언
                "    TYPE t_id IS TABLE OF customer.id%TYPE INDEX BY BINARY_INTEGER; " +
                "    TYPE t_email IS TABLE OF customer.email%TYPE INDEX BY BINARY_INTEGER; " +
                "    TYPE t_credit_limit IS TABLE OF customer.credit_limit%TYPE INDEX BY BINARY_INTEGER; " +
                "    TYPE t_address IS TABLE OF customer.address1%TYPE INDEX BY BINARY_INTEGER; " +
                "    TYPE t_enroll_dt IS TABLE OF customer.enroll_dt%TYPE INDEX BY BINARY_INTEGER; " +
                "    TYPE t_gender IS TABLE OF customer.gender%TYPE INDEX BY BINARY_INTEGER; " +
                "    TYPE t_coupon IS TABLE OF bonus_coupon.coupon%TYPE INDEX BY BINARY_INTEGER; " +
                // 배열 크기 설정
                "    v_arraysize NUMBER(10) := 1000; " +
                // 배열 변수 선언
                "    v_id t_id; " +
                "    v_email t_email; " +
                "    v_credit_limit t_credit_limit; " +
                "    v_address t_address; " +
                "    v_enroll_dt t_enroll_dt; " +
                "    v_gender t_gender; " +
                "    v_coupon t_coupon; " +
                // 처리한 레코드 수를 세기 위한 변수
                "    v_counter BINARY_INTEGER := 0; " +
                "BEGIN " +
                "    OPEN bonus_cur; " + // 커서 열기
                "    LOOP " +
                // 커서에서 데이터를 배열로 BULK COLLECT로 가져오기
                "        FETCH bonus_cur BULK COLLECT INTO v_id, v_email, v_credit_limit, v_address, v_enroll_dt, v_gender LIMIT v_arraysize; " +
                "        EXIT WHEN v_id.COUNT = 0; " + // 더 이상 가져올 데이터가 없으면 루프 종료
                "        " +
                // 가져온 데이터에 대해 쿠폰 조건 처리
                "        FOR i IN 1..v_id.COUNT LOOP " +
                "            IF v_credit_limit(i) < 1000 THEN " +
                "                v_coupon(i) := 'AA'; " +
                "            ELSIF v_credit_limit(i) >= 1000 AND v_credit_limit(i) <= 2999 THEN " +
                "                v_coupon(i) := 'BB'; " +
                "            ELSIF v_credit_limit(i) >= 3000 AND v_credit_limit(i) <= 3999 THEN " +
                "                IF v_address(i) LIKE '%송파구 풍납1동%' AND v_gender(i) = 'F' THEN " +
                "                    v_coupon(i) := 'C2'; " +
                "                ELSE " +
                "                    v_coupon(i) := 'CC'; " +
                "                END IF; " +
                "            ELSE " +
                "                v_coupon(i) := 'DD'; " +
                "            END IF; " +
                "        END LOOP; " +
                "        " +
                // FORALL을 사용하여 배열에 있는 데이터를 한 번에 INSERT
                "        FORALL i IN 1..v_id.COUNT " +
                "            INSERT INTO bonus_coupon(yyyymm, customer_id, email, coupon, credit_point, send_dt, receive_dt, use_dt) " +
                "            VALUES (?, v_id(i), v_email(i), v_coupon(i), v_credit_limit(i), SYSDATE, NULL, NULL); " +
                "        " +
                // 10,000개의 레코드를 처리할 때마다 COMMIT
                "        v_counter := v_counter + v_id.COUNT; " +
                "        IF v_counter >= 10000 THEN " +
                "            COMMIT; " +
                "            v_counter := 0; " +
                "        END IF; " +
                "    END LOOP; " +
                "    " +
                "    CLOSE bonus_cur; " + // 커서 닫기
                "    COMMIT; " + // 마지막 커밋
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
