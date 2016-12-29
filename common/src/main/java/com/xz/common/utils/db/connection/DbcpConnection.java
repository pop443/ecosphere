package com.xz.common.utils.db.connection;

import com.xz.common.utils.db.factory.DBCenter;
import com.xz.common.utils.db.factory.po.DBConfig;
import org.apache.commons.dbcp.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * falcon -- 2016/11/24.
 */
public class DbcpConnection extends DBCenter {
    private Map<String,BasicDataSource> basicDataSourceMap ;
    public DbcpConnection(Map<String, DBConfig> map) {
        super(map);
        basicDataSourceMap = new HashMap<>() ;
        for (Map.Entry<String, DBConfig> entry:map.entrySet()) {
            DBConfig dbConfig = entry.getValue() ;
            BasicDataSource basicDataSource = new BasicDataSource();
            basicDataSource.setDriverClassName("org.gjt.mm.mysql.Driver");
            basicDataSource.setUrl(dbConfig.getUrl()+dbConfig.getDatabase());
            basicDataSource.setUsername(dbConfig.getUsername());
            basicDataSource.setPassword(dbConfig.getPassword()) ;
            basicDataSource.setMaxActive(dbConfig.getMaxActive());
            basicDataSource.setMaxIdle(dbConfig.getMaxIdle());
            basicDataSource.setMaxWait(dbConfig.getMaxWait());
            basicDataSourceMap.put(entry.getKey(),basicDataSource) ;
        }
    }

    @Override
    public Connection connection(String database) {
        BasicDataSource basicDataSource = basicDataSourceMap.get(database) ;
        Connection connection = null ;
        try {
            connection = basicDataSource.getConnection() ;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
