package com.xz.phoenix.conf;

import java.sql.*;

/**
 * falcon -- 2017/2/23.
 */
public class PhoenixConnectionUtil {
    private static  String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
    private static String url = "jdbc:phoenix:vggapp19,vggapp20,vggapp29";
    static{
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection(){
        Connection conn = null ;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn ;
    }

    public static void release(Connection conn){
        try {
            if (conn!=null && !conn.isClosed()){
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void release(PreparedStatement pstm){
        try {
            if ( pstm!=null && !pstm.isClosed() ){
                pstm.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void release(ResultSet rs){
        try {
            if ( rs!=null && !rs.isClosed() ){
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void release(PreparedStatement pstm ,Connection conn){
        PhoenixConnectionUtil.release(pstm);
        PhoenixConnectionUtil.release(conn);
    }
    public static void release(ResultSet rs ,Connection conn){
        PhoenixConnectionUtil.release(rs);
        PhoenixConnectionUtil.release(conn);
    }

    public static void release(ResultSet rs , PreparedStatement pstm ,Connection conn){
        PhoenixConnectionUtil.release(rs);
        PhoenixConnectionUtil.release(pstm,conn);
    }

}
