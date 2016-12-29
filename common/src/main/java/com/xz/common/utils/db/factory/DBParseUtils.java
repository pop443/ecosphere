package com.xz.common.utils.db.factory;

import com.xz.common.utils.db.exception.DBParseException;
import com.xz.common.utils.db.factory.po.DBConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * falcon -- 2016/11/24.
 */
public class DBParseUtils {
    public static Pattern pattern = Pattern.compile(" ") ;

    public static Map<String,DBConfig> parse(Properties properties) throws DBParseException {
        Map<String,DBConfig> map = new HashMap<>() ;
        String databaseNames = properties.getProperty("database") ;
        String[] databases = pattern.split(databaseNames) ;
        for (String database:databases) {
            String url = properties.getProperty(database+".url") ;
            String username = properties.getProperty(database+".username") ;
            String password = properties.getProperty(database+".password") ;
            String maxActive = properties.getProperty(database+".maxActive","30") ;
            String maxIdle = properties.getProperty(database+".maxActive","10") ;
            String maxWait = properties.getProperty(database+".maxWait","3600000") ;
            DBConfig dbConfig = new DBConfig(database,url,username,password,maxActive,maxIdle,maxWait) ;
            map.put(database,dbConfig);
        }
        if (map.size()==0){
            throw new DBParseException("配置文件为空") ;
        }
        return map ;
    }

}
