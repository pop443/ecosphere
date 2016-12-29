package com.xz.hive.utils;

/**
 * falcon -- 2016/12/27.
 */
import java.sql.*;

public class DbTest {
    public static void main(String[] args)
            throws SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection conn = DriverManager.getConnection(
                "jdbc:hive2://172.32.148.165:10000/default", "", "");
        String sql = "describe formatted  test";
        PreparedStatement pstm = conn.prepareStatement(sql);
        ResultSet rs = pstm.executeQuery() ;
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        rs.close();
        pstm.close();
        conn.close();
    }
}
