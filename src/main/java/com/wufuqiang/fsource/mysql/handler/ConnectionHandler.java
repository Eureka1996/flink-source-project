package com.wufuqiang.fsource.mysql.handler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class ConnectionHandler {

    private static String urlFormat =
            "jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

    public static Connection getConnection(String driver,String hostname,String port,
                                           String username ,String password,String database){
        Connection conn = null;
        try{
            Class.forName(driver);
            conn = DriverManager.getConnection(
                    String.format(urlFormat, hostname, port, database),
                    username, password);
        }catch (Exception e){
            e.printStackTrace();
        }
        return conn;
    }

    public static Connection getConnection(Properties properties) {
        Connection conn = null;
        try{
            Class.forName(properties.getProperty("database.driver"));
            conn = DriverManager.getConnection(
                    String.format(urlFormat,
                            properties.getProperty("database.hostname"),
                            properties.getProperty("database.port"),
                            properties.getProperty("database.dbname")),
                    properties.getProperty("database.username"), properties.getProperty("database.password"));
        }catch (Exception e){
            e.printStackTrace();
        }
        return conn;
    }
}
