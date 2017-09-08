package com.xz.phoenix.demo1;

import com.xz.phoenix.utils.PhoenixConnectionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * falcon -- 2017/2/23.
 */
public class CreateTable {
    private String tableName = "\"hbase\"" ;

    public boolean createTable() {
        boolean bo = false;
        Connection conn = null;
        PreparedStatement pstm = null;
        StringBuilder sb = new StringBuilder() ;
        sb.append("create table IF NOT EXISTS ").append(tableName).append("(ROW varchar not null primary key,");
        for (int i = 0; i <20 ; i++) {
            sb.append("\"cf1\".\"").append(i).append("\" varchar");
            if (i!=19){
                sb.append(",");
            }
        }
        sb.append(")") ;
        System.out.println(sb.toString());
        try {
            conn = PhoenixConnectionUtil.getConnection();
            pstm = conn.prepareStatement(sb.toString());
            bo = pstm.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            PhoenixConnectionUtil.release(pstm, conn);
        }
        return bo;
    }

    public void select1(){
        Connection conn = null;
        PreparedStatement pstm = null;
        ResultSet rs = null ;
        try {
            conn = PhoenixConnectionUtil.getConnection();
            pstm = conn.prepareStatement("select * from PHOENIX_TEST");
            rs = pstm.executeQuery() ;
            while (rs.next()){
                for (int i = 0; i < 2 ; i++) {
                    System.out.print(rs.getString(i+1)+"--");
                }
                System.out.println("");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            PhoenixConnectionUtil.release(rs,pstm, conn);
        }
    }

    public static void main(String[] args) {
        CreateTable demo1 = new CreateTable();
        //System.out.println(demo1.createTable());
        demo1.select1();

    }
}
