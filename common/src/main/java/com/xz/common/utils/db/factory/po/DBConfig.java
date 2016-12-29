package com.xz.common.utils.db.factory.po;

/**
 * falcon -- 2016/11/24.
 */
public class DBConfig {
    private String database ;
    private String url ;
    private String username;
    private String password ;
    private String maxActive ;
    private String maxIdle ;
    private String maxWait ;

    public DBConfig(String database, String url, String username, String password, String maxActive, String maxIdle, String maxWait) {
        this.database = database;
        this.url = url;
        this.username = username;
        this.password = password;
        this.maxActive = maxActive;
        this.maxIdle = maxIdle;
        this.maxWait = maxWait;
    }

    public String getDatabase() {
        return database;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getMaxActive() {
        int i = 0 ;
        try {
            i = Integer.parseInt(maxActive) ;
        } catch (NumberFormatException e) {
            e.printStackTrace();
            i = 30 ;
        }
        return i;
    }

    public int getMaxIdle() {
        int i = 0 ;
        try {
            i = Integer.parseInt(maxIdle) ;
        } catch (NumberFormatException e) {
            e.printStackTrace();
            i = 10 ;
        }
        return i;
    }

    public long getMaxWait() {
        long i = 0 ;
        try {
            i = Long.parseLong(maxWait) ;
        } catch (NumberFormatException e) {
            e.printStackTrace();
            i = 3600000 ;
        }
        return i;
    }
}
