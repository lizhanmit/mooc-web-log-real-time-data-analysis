package com.zhandev.spark.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase operation utils
 * It is recommanded using singleton for Java utlis.
 */
public class HBaseUtils {

    HBaseAdmin admin = null;
    Configuration configuration = null;

    private static HBaseUtils hBaseUtilsObj;

    // private constructor, singleton
    private HBaseUtils() {
        configuration = new Configuration();
        // these two values can be accessed from "/usr/local/hbase/conf/habse-site.xml"
        configuration.set("hbase.zookeeper.quorum", "localhost:2181");
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Lazy
     */
    public static synchronized HBaseUtils getInstance() {
        if (hBaseUtilsObj == null) {
            hBaseUtilsObj = new HBaseUtils();
        }
        return hBaseUtilsObj;
    }

    /**
     * Get HTable obj by table name.
     *
     * @param tableName  Table name.
     * @return  HTable obj.
     */
    public HTable getTable(String tableName) {
        HTable table = null;
        try {
            table = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * Insert a record of data into HBase.
     *
     * @param tableName
     * @param rowkey
     * @param columnFamilyName
     * @param columnName
     * @param value
     */
    public void put(String tableName, String rowkey, String columnFamilyName, String columnName, String value) {
        HTable table = HBaseUtils.getInstance().getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * For testing functions.
     */
    public static void main(String[] args) {
        //HTable table = HBaseUtils.getInstance().getTable("mooc_course_clickcount");
        //System.out.println(table.getName().getNameAsString());

        //HBaseUtils.getInstance().put("mooc_course_clickcount", "20180803_123", "info", "click_count", "10");

        HBaseUtils.getInstance().put("mooc_course_search_clickcount", "20180803_www.google.com_123", "info", "click_count", "10");

    }


}
