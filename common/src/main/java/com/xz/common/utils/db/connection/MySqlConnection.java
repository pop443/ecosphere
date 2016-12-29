package com.xz.common.utils.db.connection;

import com.xz.common.utils.db.DBUtil;
import com.xz.common.utils.db.factory.DBCenter;
import com.xz.common.utils.db.factory.po.DBConfig;

import java.sql.*;
import java.util.Map;

/**
 * falcon -- 2016/11/24.
 */
public class MySqlConnection extends DBCenter {

    public MySqlConnection(Map<String, DBConfig> map) {
        super(map);
        try {
            Class.forName("com.connection.jdbc.Driver") ;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("init MySqlConnection");
    }
    @Override
    public Connection connection(String database) {
        DBConfig dbConfig = super.map.get(database) ;
        Connection connection = null ;
        try {
            connection = DriverManager.getConnection(dbConfig.getUrl()+dbConfig.getDatabase(),dbConfig.getUsername(),dbConfig.getPassword());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return  connection ;
    }
}
