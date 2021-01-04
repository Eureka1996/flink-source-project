package com.wufuqiang.fsource.mysql;

import org.apache.flink.util.Preconditions;

import java.util.Properties;

public class MySqlJDBCSource {

    public MySqlJDBCSource(){

    }

    public static <T> MySqlJDBCSource.Builder<T> builder(){
        return new MySqlJDBCSource.Builder();
    }

    public static class Builder<T>{
        private String driver = "com.mysql.cj.jdbc.Driver";
        private int port = 3306;
        private String hostname ;
        private String username;
        private String password;
        private String dbname;


        private String sql = "select * from websites where id > 10 ;";


        public Builder(){

        }

        public MySqlJDBCSource.Builder<T> driver(String driver){
            this.driver = driver;
            return this;
        }

        public MySqlJDBCSource.Builder<T> hostname(String hostname){
            this.hostname = hostname;
            return this;
        }

        public MySqlJDBCSource.Builder<T> port(int port){
            this.port = port;
            return this;
        }

        public MySqlJDBCSource.Builder<T> username(String username){
            this.username = username;
            return this;
        }

        public MySqlJDBCSource.Builder<T> password(String password){
            this.password = password;
            return this;
        }

        public MySqlJDBCSource.Builder<T> sql(String sql){
            this.sql = sql;
            return this;
        }

        public MySqlJDBCSource.Builder<T> dbname(String dbname){
            this.dbname = dbname;
            return this;
        }



        public MySqlJDBCSourceFunction<T> build(){
            Properties properties = new Properties();
            properties.setProperty("database.driver", (String)Preconditions.checkNotNull(this.driver));
            properties.setProperty("database.hostname", (String)Preconditions.checkNotNull(this.hostname));
            properties.setProperty("database.port", String.valueOf(this.port));
            properties.setProperty("database.username", (String)Preconditions.checkNotNull(this.username));
            properties.setProperty("database.password", (String)Preconditions.checkNotNull(this.password));
            properties.setProperty("query.sql",(String)Preconditions.checkNotNull(this.sql));
            properties.setProperty("database.dbname",(String)Preconditions.checkNotNull(this.dbname));
            return new MySqlJDBCSourceFunction(properties);
        }
    }

}
