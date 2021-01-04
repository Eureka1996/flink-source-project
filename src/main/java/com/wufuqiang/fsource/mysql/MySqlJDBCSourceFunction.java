package com.wufuqiang.fsource.mysql;

import com.wufuqiang.fsource.mysql.handler.ConnectionHandler;
import com.wufuqiang.fsource.mysql.handler.ResultSetHandler;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.lang.reflect.ParameterizedType;
import java.sql.*;
import java.util.Properties;

public class MySqlJDBCSourceFunction<T> extends RichSourceFunction<T> implements ResultTypeQueryable<T> {

    private final Properties properties;
    private Connection conn;
    private PreparedStatement pstmt ;
    private DeserializationSchema<T> deserializer;




    public MySqlJDBCSourceFunction(Properties properties) {
        this.properties = properties;
//        deserializer = new De
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.conn = ConnectionHandler.getConnection(this.properties);
        try {
            pstmt = conn.prepareStatement(this.properties.getProperty("query.sql"));
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        ResultSet resultSet = pstmt.executeQuery();
        Class<T> clazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        ResultSetHandler.resultToCtx(resultSet,clazz,sourceContext);
    }

    @Override
    public void cancel() {


    }

    @Override
    public void close() throws Exception {
        super.close();
        if(conn != null){
            conn.close();
        }
        if(pstmt != null){
            pstmt.close();
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return null;
    }
}
