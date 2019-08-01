package com.ljy.hadoop.study.hbase_test.demo;

import com.ljy.hadoop.study.hbase_test.demo.model.Student;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.ws.rs.PUT;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class HbaseDemo {
    public static Configuration conf;
    static{
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node01,node02,node03");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    /**
     * 检查表是否存在
     */
    public boolean exitsTable(String tableName) throws IOException{
        //通过连接获取hbase客户端对象
        Connection connection= ConnectionFactory.createConnection(conf);
        //通过连接获取hbase客户端对象
        Admin admin = connection.getAdmin();
        //通过admin操作hbase
        boolean res = admin.tableExists(TableName.valueOf(tableName));
        return res;
    }

    /**
     * 创建表格和列族描述器
     * @param tableName 表名
     * @param columnNames 列族描述器，可以有多个
     * @throws IOException
     */
    public void createTable(String tableName,String... columnNames) throws IOException {
        //通过连接获取hbase客户端对象
        Connection connection= ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if(!admin.tableExists(TableName.valueOf(tableName))){//判断该表是否存在
            //创建表描述器
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //设置列族描述器
            for(String columnName:columnNames){
                hTableDescriptor.addFamily(new HColumnDescriptor(columnName));
            }
            //执行创建操作
            admin.createTable(hTableDescriptor);
            System.out.println(tableName+"表创建成功！！");
        }else{
            System.out.println(tableName+"表已经存在，无需创建！！");
        }
    }




    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    public void deleteTable(String tableName) throws IOException{
        //通过连接获取hbase客户端对象
        Connection connection= ConnectionFactory.createConnection(conf);
        //通过连接获取hbase客户端对象
        Admin admin = connection.getAdmin();
        //先设置表为不可用，disable
        admin.disableTable(TableName.valueOf(tableName));
        //删除表
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println(tableName+"表删除成功！！");
    }


    /**
     * 获取该表所有记录信息
     * @param tableName
     * @throws IOException
     */
    public void scanData(String tableName) throws IOException{
        //通过连接获取hbase客户端对象
        Connection connection= ConnectionFactory.createConnection(conf);
        //通过连接获取hbase客户端对象
        Admin admin = connection.getAdmin();
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //获取一个scan对象
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        //遍历scanner
        for (Result res : scanner) {
            //获取到一行数据
            Cell[] cells = res.rawCells();//获取该行的所有cell对象
            for (Cell cell : cells) {
                //通过cell获取rowkey,cf,column,value
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));//列簇名
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));//列名
                String value = Bytes.toString(CellUtil.cloneValue(cell));//值
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));//rowkey显示不出
                System.out.println(rowkey + "----" + cf + "---" + column + "---" + value);
            }
        }
        table.close();//关闭表对象
    }

    /**
     * 获取该表相应列簇的信息
     * @param tableName 表名
     * @param columnName 列簇名
     * @param name1  列簇下的字段名，可以为Null。如果为null，则查询列簇下的所有字段信息
     * @param rowkey 行键
     * @throws IOException
     */
    public void scanDataByCF(String tableName,String columnName,String name1,Object rowkey) throws IOException{
        //通过连接获取hbase客户端对象
        Connection connection= ConnectionFactory.createConnection(conf);
        //通过连接获取hbase客户端对象
        Admin admin = connection.getAdmin();
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建查询的get对象
        Get get = new Get(Bytes.toBytes(rowkey.toString()));
        //指定列族信息
        if(name1==null){
            get.addFamily(Bytes.toBytes(columnName));
        }else{
            get.addColumn(Bytes.toBytes(columnName), Bytes.toBytes(name1));
        }
        //执行查询
        Result res = table.get(get);
        Cell[] cells = res.rawCells();//获取改行的所有cell对象
        //遍历scanner
        for (Cell cell : cells) {
            //通过cell获取rowkey,cf,column,value
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));//列簇名
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));//列名
            String value = Bytes.toString(CellUtil.cloneValue(cell));//值
            System.out.println(rowkey + "----" + cf + "---" + column + "---" + value);
        }
        table.close();//关闭表对象
    }


    /**
     * 插入学生集合进数据库
     * @param tableName 表名
     * @param students  学生信息集合
     * @throws IOException
     */
    public void putStudents(String tableName, List<Student> students) throws IOException{
        //通过连接获取hbase客户端对象
        Connection connection= ConnectionFactory.createConnection(conf);
        //通过连接获取hbase客户端对象
        Admin admin = connection.getAdmin();
        //获取一个表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Put> puts=new ArrayList<Put>();//定义集合用于后续批量插入
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyyMMdd");
        for(Student student:students){
            //设定rowkey
            Put put = new Put(Bytes.toBytes(student.getId()));
            //列族，列，value。这里输入姓名
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(student.getName()));
            //列族，列，value。这里输入生日
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birth"), Bytes.toBytes(dateFormat.format(student.getBirth())));
            puts.add(put);
        }
        //执行插入
        table.put(puts);
        //关闭table对象
        table.close();
        System.out.println("插入成功！！");
    }

    /**
     * 在指定表中删除指定行
     * @param tableName 表名
     * @param rowkey
     * @throws IOException
     */
    public void deleteData(String tableName,Object rowkey) throws IOException{
        //通过连接获取hbase客户端对象
        Connection connection= ConnectionFactory.createConnection(conf);
        //通过连接获取hbase客户端对象
        Admin admin = connection.getAdmin();
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建一个delete对象
        Delete delete = new Delete(Bytes.toBytes("2"));
        table.delete(delete);
        table.close();
        System.out.println("删除指定数据成功！");
    }

    /**
     * 在指定表中删除指定行
     * @param tableName 表名
     * @param rowkeys
     * @throws IOException
     */
    public void deleteDatas(String tableName,Object... rowkeys) throws IOException{
        //通过连接获取hbase客户端对象
        Connection connection= ConnectionFactory.createConnection(conf);
        //通过连接获取hbase客户端对象
        Admin admin = connection.getAdmin();
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<Delete>();
        for(Object rowkey:rowkeys){
            //创建一个delete对象
            Delete delete = new Delete(Bytes.toBytes(rowkey.toString()));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
        System.out.println("批量删除指定数据成功！");
    }


}
