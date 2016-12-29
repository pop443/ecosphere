package com.xz.common;

import com.xz.common.utils.db.DBUtil;

import java.sql.*;

/**
 * falcon -- 2016/11/29.
 */
public class DBTest {
    public static void main(String[] args) {
        try {
            Connection conn = DBUtil.getConnection("ambari") ;
            DatabaseMetaData databaseMetaData = conn.getMetaData() ;
            ResultSet rs = databaseMetaData.getTables(null,null,null,null);
            ResultSetMetaData rsmd = rs.getMetaData() ;
            for(int i = 1;i<=rsmd.getColumnCount();i++){
                System.out.println(rsmd.getColumnName(i));
            }
            while (rs.next()){
                System.out.println(rs.getString("TABLE_NAME"));
            }

            rs.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            Connection conn = DBUtil.getConnection("hive") ;
            DatabaseMetaData databaseMetaData = conn.getMetaData() ;
            ResultSet rs = databaseMetaData.getTables(null,null,null,null);
            ResultSetMetaData rsmd = rs.getMetaData() ;
            for(int i = 1;i<=rsmd.getColumnCount();i++){
                System.out.println(rsmd.getColumnName(i));
            }
            while (rs.next()){
                System.out.println(rs.getString("TABLE_NAME"));
            }

            DBUtil.release(rs,conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
