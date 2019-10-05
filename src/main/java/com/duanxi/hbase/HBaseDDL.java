package com.duanxi.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author caoduanxi
 * @2019/10/5 10:51
 * HBase的javaAPI的认识
 * 实现基础的方法以及表的增删改
 */
public class HBaseDDL {
    Connection connection;
    public Admin admin;
    HTable table;
    String tableName = "phone";
    String familyNames[] = {"cf"};

    /**
     * 实现HBase的初始化操作
     */
    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node02,node03,node04");
        connection = ConnectionFactory.createConnection(conf);
        // 构建HBaseAdmin
        admin = connection.getAdmin();
    }

    /**
     * 创建表
     */
    @Test
    public void createTable() throws Exception {
        // 如果表存在则先删除表，然后创建
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }
        // 获取表描述
        TableDescriptorBuilder descriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

        // 获取列描述
        ColumnFamilyDescriptor cd = ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes()).build();
        // 如果是多个的话，采用familyNames遍历的形式
//        for (String familyName : familyNames) {
//
//        }
        descriptor.setColumnFamily(cd);
        admin.createTable(descriptor.build());
    }

    /**
     * 添加数据/修改数据是直接覆盖，等同
     */
    @Test
    public void putData() throws Exception {
        // 首先获取到表
        Table table = connection.getTable(TableName.valueOf("test01"));
        String rowKey = "row1";
        Put put = new Put(rowKey.getBytes());
        put.addColumn("cf".getBytes(), "name".getBytes(), "caoduanxi".getBytes());
        put.addColumn("cf".getBytes(), "age".getBytes(), "23".getBytes());
        put.addColumn("cf".getBytes(), "sex".getBytes(), "boy".getBytes());
        table.put(put);
    }

    /**
     * 获取数据
     *
     * @throws Exception
     */
    @Test
    public void getData() throws Exception {
        // 使用get可以获取任何的单行数据
        Get get = new Get("row1".getBytes());
        get.addColumn("cf".getBytes(), "name".getBytes());
        get.addColumn("cf".getBytes(), "age".getBytes());
        get.addColumn("cf".getBytes(), "sex".getBytes());
        // 获取表
        Table table = connection.getTable(TableName.valueOf("test01"));
        Result result = table.get(get);
        // 获取cell单元格中的数据
        Cell cell1 = result.getColumnLatestCell("cf".getBytes(), "name".getBytes());
        Cell cell2 = result.getColumnLatestCell("cf".getBytes(), "age".getBytes());
        Cell cell3 = result.getColumnLatestCell("cf".getBytes(), "sex".getBytes());
        System.out.println(new String(CellUtil.cloneValue(cell1)));
        System.out.println(new String(CellUtil.cloneValue(cell2)));
        System.out.println(new String(CellUtil.cloneValue(cell3)));
    }

    Random random = new Random();
    SimpleDateFormat sf = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * 通话记录存储表
     * 十个用户，每个用户每天产生100条通话记录
     *
     * @throws Exception
     */
    @Test
    public void insertDB() throws Exception {
        List<Put> puts = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String phoneNum = getPhoneNum("157");
            for (int j = 0; j < 100; j++) {
                String dnum = getPhoneNum("132");
                String length = random.nextInt(99) + "";
                String type = random.nextInt(2) + "";
                String dateStr = getDateStr("2019");
                // 设计rowkey,实现按照时间倒序排列
                String rowKey = phoneNum + "_" + (Long.MAX_VALUE - sf.parse(dateStr).getTime());
                Put put = new Put(rowKey.getBytes());
                put.addColumn("cf".getBytes(), "dnum".getBytes(), dnum.getBytes());
                put.addColumn("cf".getBytes(), "length".getBytes(), length.getBytes());
                put.addColumn("cf".getBytes(), "type".getBytes(), type.getBytes());
                put.addColumn("cf".getBytes(), "dateStr".getBytes(), dateStr.getBytes());
                puts.add(put);
            }
        }
        Table table = connection.getTable(TableName.valueOf("phone"));
        table.put(puts);
    }

    /**
     * 获取随机时间字符串
     *
     * @param s
     * @return
     */
    private String getDateStr(String s) {
        return s + String.format("%02d%02d%02d%02d%02d", new Object[]{random.nextInt(12) + 1,
                random.nextInt(30) + 1, random.nextInt(24) + 1, random.nextInt(60), random.nextInt(60)
        });
    }

    @Test
    public void test() {
        System.out.println(getDateStr("2019"));
        System.out.println(getDateStr("2018"));
    }

    /**
     * 获取随机的电话号码
     *
     * @param s
     * @return
     */
    private String getPhoneNum(String s) {
        return s + String.format("%08d", random.nextInt(99999999));
    }

    /**
     * 统计二月份到三月份的通话记录
     */
    @Test
    public void scan() throws Exception {
        String phoneNum = "15798412711";
        String startRow = phoneNum + "_" + (Long.MAX_VALUE - sf.parse("20190301000000").getTime());
        String stopRow = phoneNum + "_" + (Long.MAX_VALUE - sf.parse("20190201000000").getTime());

        Scan scan = new Scan();
        scan.withStartRow(startRow.getBytes());
        scan.withStopRow(stopRow.getBytes());

        Table table = connection.getTable(TableName.valueOf("phone"));
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            System.out.print(new String(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
            System.out.print("-" + new String(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
            System.out.print("-" + new String(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
            System.out.println("-" + new String(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "dateStr".getBytes()))));
        }
    }

    /**
     * 获取某号码的type为1的通话记录
     */
    @Test
    public void getType() throws Exception {
        String phoneNum = "15798412711";
        // 前缀过滤器
        PrefixFilter filter = new PrefixFilter(phoneNum.getBytes());
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        // 单列值选择过滤器
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
                "cf".getBytes(),
                "type".getBytes(),
                CompareOperator.EQUAL,
                "1".getBytes()
        );
        // 添加过滤器
        list.addFilter(filter);
        list.addFilter(filter1);
        Scan scan = new Scan();
        scan.setFilter(list);
        Table table = connection.getTable(TableName.valueOf("phone"));
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            System.out.print(new String(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
            System.out.print("-" + new String(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
            System.out.print("-" + new String(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
            System.out.println("-" + new String(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "dateStr".getBytes()))));
        }

    }

    /**
     * 删除表
     */
    @Test
    public void deleteTable() throws Exception {
        Table table = connection.getTable(TableName.valueOf("test01"));
        Delete delete = new Delete("row1".getBytes());
        table.delete(delete);
    }


    @After
    public void destroy() throws IOException {
        if (admin != null) {
            admin.close();
        }
    }
}
