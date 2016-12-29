package com.xz.common.utils.db;

import com.xz.common.utils.db.factory.DBFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * falcon -- 2016/11/24.
 */
public class DBUtil {
    private static DBFactory dbFactory = DBFactory.newInstance() ;

    public static Connection getConnection(String database){
        return dbFactory.getConnection(database) ;
    }

    public static void release(Connection conn){
        try {
            if ( conn!=null && !conn.isClosed() ){
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
        DBUtil.release(pstm);
        DBUtil.release(conn);
    }
    public static void release(ResultSet rs ,Connection conn){
        DBUtil.release(rs);
        DBUtil.release(conn);
    }

    public static void release(ResultSet rs , PreparedStatement pstm ,Connection conn){
        DBUtil.release(rs);
        DBUtil.release(pstm,conn);
    }
}
