package hbase.app;

import hbase.Constant.Constant;
import hbase.base.BaseDaoImpl;
import hbase.students.StudentsServiceImpl;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.nio.charset.Charset;
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

        get.addColumn(Constant.FAMILY_NAME_1,Constant.CLOUMN1);
        get.addColumn(Constant.FAMILY_NAME_1,Constant.CLOUMN2);
        get.addColumn(Constant.FAMILY_NAME_1,Constant.CLOUMN3);

        Result rs = baseDao.getData(get, Constant.TABLE_NAME);
        //查最新的版本
        Cell cell = rs.getColumnLatestCell(Constant.FAMILY_NAME_1,Constant.CLOUMN1);
        System.out.println(new String(CellUtil.cloneValue(cell),"utf-8"));

    }

    /**
     * 插入数据 排序
     */
    public void descDate(){

    }

}
