package com.ljy.hadoop.study.hbase_test.demo;



import com.ljy.hadoop.study.hbase_test.demo.model.Student;
import org.apache.avro.generic.GenericData;
import org.testng.annotations.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class HbaseDemoTest {

    private HbaseDemo hbaseDemo =new HbaseDemo();

    @Test
    public void testCreateTable() {
        try {
            hbaseDemo.createTable("student_test","info");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //判断hbase表是否存在
    @Test
    public void testTableExits() throws IOException {
        System.out.println("student表是否存在：" + hbaseDemo.exitsTable("student1"));
    }

    @Test
    public void testDeleteTable() throws IOException {
        if(hbaseDemo.exitsTable("student_test")){
            hbaseDemo.deleteTable("student_test");
        }
    }

    @Test
    public void testScanData() throws IOException{
        hbaseDemo.scanData("student_test");
    }

    @Test
    public void testScanDataByCF() throws IOException{
        hbaseDemo.scanDataByCF("student_test","info","name","2");
        hbaseDemo.scanDataByCF("student_test","info",null,"2");
    }

    @Test
    public void testDeleteData() throws IOException, ParseException {
        hbaseDemo.deleteData("student_test",2);
    }

    @Test
    public void testDeleteDatas() throws IOException, ParseException {
        hbaseDemo.deleteDatas("student_test",new String[]{"1","3"});
    }

    @Test
    public void testPutStudents() throws IOException, ParseException {
        if(hbaseDemo.exitsTable("student_test")){
            List<Student> students=new ArrayList<Student>();
            SimpleDateFormat dateFormat=new SimpleDateFormat("yyyyMMdd");
            students.add(new Student("1","张三",dateFormat.parse("20150119")));
            students.add(new Student("2","李四",dateFormat.parse("20171212")));
            students.add(new Student("3","小红",dateFormat.parse("20180605")));
            hbaseDemo.putStudents("student_test",students);
        }
    }


}
