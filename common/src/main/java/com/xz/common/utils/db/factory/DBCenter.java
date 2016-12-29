package com.xz.common.utils.db.factory;

import com.xz.common.utils.db.factory.po.DBConfig;

import java.sql.Connection;
import java.util.Map;

/**
 * falcon -- 2016/11/24.
 */
public abstract class DBCenter {
    protected Map<String,DBConfig> map ;

    public DBCenter(Map<String, DBConfig> map) {
        this.map = map;
    }
    public abstract Connection connection(String database) ;
}
