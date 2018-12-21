package com.soul.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author soulChun
 * @create 2018-12-19-09:43
 */
public class SQLUtils {

    public List<String> getColumns(String querysql){
        List<String> column = new ArrayList<String>();
        String tmp = querysql.substring(querysql.indexOf("select") + 6,
                querysql.indexOf("from")).trim();
        if (tmp.indexOf("*") == -1){
            String cols[] = tmp.split(",");
            for (String c:cols){
                column.add(c);
            }
        }
        return column;
    }

    public String getTBname(String querysql){
        String tmp = querysql.substring(querysql.indexOf("from")+4).trim();
        int sx = tmp.indexOf(" ");
        if(sx == -1){
            return tmp;
        }else {
            return tmp.substring(0,sx);
        }
    }




    // 泛型方法 printArray
    public static < E > void printArray( E[] inputArray )
    {
        // 输出数组元素
        for ( E element : inputArray ){
            System.out.printf( "%s ", element );
        }
        System.out.println();
    }

}
