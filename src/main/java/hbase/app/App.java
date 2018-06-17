package hbase.app;

import hbase.Constant.Constant;
import hbase.base.BaseDaoImpl;
import hbase.students.StudentsServiceImpl;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @program: hbase-demo
 * @GitHub: https://github.com/ABHSY
 * @author: ABHSY.Klein
 * @create: 2018-06-17 21:11
 **/
public class App {


    @Test
    public void testCreateTable() throws Exception {
        StudentsServiceImpl ssi = new StudentsServiceImpl();
        ssi.createStuTable();
    }

    @Test
    public void testPutDate() throws Exception {
        StudentsServiceImpl ssi = new StudentsServiceImpl();
        //添加数据
        //列名/值
        Map<byte[],byte[]> bytes = new HashMap<byte[],byte[]>();
        bytes.put(Constant.CLOUMN1, Bytes.toBytes("Jack"));
        bytes.put(Constant.CLOUMN2,Bytes.toBytes("10"));
        bytes.put(Constant.CLOUMN3,Bytes.toBytes("O:90,T:89,S:100"));
        ssi.putStuData("cf1",bytes);
    }

    @Test
    public void testGetDate() throws Exception {
        BaseDaoImpl baseDao = new BaseDaoImpl();
        //添加row key
        Get get = new Get(Constant.STU_ROW_NAME.getBytes());
        //添加列族
        get.addColumn(Constant.FAMILY_NAME_1,Constant.CLOUMN1);

        Result rs = baseDao.getData(get, Constant.TABLE_NAME);
        Cell cell = rs.getColumnLatestCell(Constant.FAMILY_NAME_1,Constant.CLOUMN1);

    }
}
