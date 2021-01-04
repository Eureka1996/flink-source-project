package com.wufuqiang.fsource.mysql.handler;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

public class ResultSetHandler {


    /**
     * 将ResultSet中的内容转化成为对象集合
     * @param rs
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> List<T> resultToList(ResultSet rs , Class<T> clazz){
        List<T> list = new ArrayList<>();
        try{
            //获取resultSet的列的信息
            ResultSetMetaData metaData = rs.getMetaData();

            //遍历ResultSet
            while (rs.next()){
                //通过反射获取对象的实例
                T t = clazz.getConstructor().newInstance();
                //遍历每一列
                for(int i = 0;i<metaData.getColumnCount();i++){
                    //获取列的名字
                    String fName = metaData.getColumnLabel(i+1);
                    //列的名称需要与属性的名称是一致的
                    Field field = clazz.getDeclaredField(fName.toLowerCase());
                    //因为属性是私有的，所以获得其对应的set方法。set+属性名首字母大写+其它小写
                    String setName = "set" +
                            fName.toUpperCase().substring(0,1) +
                            fName.substring(1).toLowerCase();

                    //因为属性的类型和set方法的参数类型一致，所以可以获得set方法
                    Method setMethod = clazz.getMethod(setName,field.getType());
                    //执行set方法，把ResultSet中的值传入到对象中，再继续传值
                    setMethod.invoke(t,rs.getObject(fName));
                }
                list.add(t);
            }

        }catch (Exception e){
            e.printStackTrace();
        }


        return list;
    }

    public static <T> void resultToCtx(ResultSet rs, Class<T> clazz, SourceFunction.SourceContext<T> ctx) {
        try{
            //获取resultSet的列的信息
            ResultSetMetaData metaData = rs.getMetaData();

            //遍历ResultSet
            while (rs.next()){
                //通过反射获取对象的实例
                T t = clazz.getConstructor().newInstance();
                //遍历每一列
                for(int i = 0;i<metaData.getColumnCount();i++){
                    //获取列的名字
                    String fName = metaData.getColumnLabel(i+1);
                    //列的名称需要与属性的名称是一致的
                    Field field = clazz.getDeclaredField(fName.toLowerCase());
                    //因为属性是私有的，所以获得其对应的set方法。set+属性名首字母大写+其它小写
                    String setName = "set" +
                            fName.toUpperCase().substring(0,1) +
                            fName.substring(1).toLowerCase();

                    //因为属性的类型和set方法的参数类型一致，所以可以获得set方法
                    Method setMethod = clazz.getMethod(setName,field.getType());
                    //执行set方法，把ResultSet中的值传入到对象中，再继续传值
                    setMethod.invoke(t,rs.getObject(fName));
                }
                ctx.collect(t);
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
