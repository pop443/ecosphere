package com.xz.common.utils.db.factory;

import com.xz.common.utils.PropertiesUtil;
import com.xz.common.utils.db.exception.DBParseException;
import com.xz.common.utils.db.factory.po.DBConfig;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.util.Map;
import java.util.Properties;

/**
 * falcon -- 2016/11/24.
 */
public class DBFactory {
    private static DBFactory dbFactory = new DBFactory() ;
    private static Map<String,DBConfig> map;
    private static DBCenter dbCenter ;

    private DBFactory(){
        System.out.println("初始化 DBFactory");
        String path = "db.properties" ;
        Properties config = PropertiesUtil.getProperties(path) ;
        try {
            String className = config.getProperty("databaseclass") ;

            Class<? extends DBCenter> cz = (Class<? extends DBCenter>) Class.forName(className);
            Constructor constructor = cz.getConstructor(Map.class) ;
            map = DBParseUtils.parse(config) ;
            dbCenter = (DBCenter)constructor.newInstance(map) ;
        } catch (DBParseException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public static DBFactory newInstance(){
        System.out.println(" DBFactory newInstance");
        return dbFactory ;
    }
    public Connection getConnection(String database){
        return dbCenter.connection(database) ;
    }
}
