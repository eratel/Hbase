package hbase.students;

import hbase.Constant.Constant;
import hbase.base.BaseConfig;
import hbase.base.BaseDao;
import hbase.base.BaseDaoImpl;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Klein
 * @Package hbase.students
 * @Description: students服务
 * @date 17/6/18 11:36
 */
public class StudentsServiceImpl {
    public BaseDao baseDao = new BaseDaoImpl();
//    public static final String TABLE_NAME = "t_students";
//    public static final String STU_ROW_NAME = "stu_row1";
//    public static final byte[] FAMILY_NAME_1 = Bytes.toBytes("cf1");
//    public static final byte[] FAMILY_NAME_2 = Bytes.toBytes("cf2");
//    public static final byte[] FAMILY_NAME_3 = Bytes.toBytes("cf3");
//    public static final byte[] CLOUMN1 = Bytes.toBytes("cf1");
//    public static final byte[] CLOUMN2 = Bytes.toBytes("cf2");
//    public static final byte[] CLOUMN3 = Bytes.toBytes("cf3");


    public void createStuTable() throws Exception{
        //判断是否存在 如果存在进行删除
//        Admin admin = BaseConfig.getConnection().getAdmin();
//        if(admin.tableExists(TableName.valueOf(TABLE_NAME))){
//            admin.disableTable(TableName.valueOf(TABLE_NAME));
//            admin.deleteTable(TableName.valueOf(TABLE_NAME));
//        }

        //创建tablename,列族1,2
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(Constant.TABLE_NAME));
        HColumnDescriptor columnDescriptor_1 = new HColumnDescriptor(Constant.FAMILY_NAME_1);
        HColumnDescriptor columnDescriptor_2 = new HColumnDescriptor(Constant.FAMILY_NAME_2);
        HColumnDescriptor columnDescriptor_3 = new HColumnDescriptor(Constant.FAMILY_NAME_3);
        tableDescriptor.addFamily(columnDescriptor_1);
        tableDescriptor.addFamily(columnDescriptor_2);
        tableDescriptor.addFamily(columnDescriptor_3);
        //添加缓存
        columnDescriptor_1.setBlockCacheEnabled(true);
        columnDescriptor_2.setBlockCacheEnabled(true);
        columnDescriptor_3.setBlockCacheEnabled(true);
        //添加内存
        columnDescriptor_1.setInMemory(true);
        columnDescriptor_2.setInMemory(true);
        columnDescriptor_3.setInMemory(true);
        //设置最大版本数
        columnDescriptor_1.setMaxVersions(1);

        baseDao.createTable(tableDescriptor);
    }

    /**
     * 插入数据  列族名称,<列名,值>
     * @param bytes
     */
    public void putStuData(String familyName,Map<byte[],byte[]> bytes) throws Exception{
        Put put =  new Put(Bytes.toBytes(Constant.STU_ROW_NAME));
        int i = 1;
        for(byte[] CloumnNames : bytes.keySet()){
            put.addColumn(familyName.getBytes(), CloumnNames, bytes.get(CloumnNames));
            i++;
        }

        baseDao.putData(put, Constant.TABLE_NAME);
    }

    public ResultScanner scanData(Map<byte[],byte[]> bytes) throws Exception{
        Scan scan = new Scan();
        for(byte[] familyNames : bytes.keySet()){
            scan.addColumn(familyNames, bytes.get(familyNames));
        }
        scan.setCaching(100);
        ResultScanner results = baseDao.scanData(scan,Constant.TABLE_NAME);

        return results;
    }
    public void delStuData(String rowId,byte[] familyName,byte[] qualifierName) throws Exception{
        Delete delete = new Delete(Bytes.toBytes(rowId));
        delete.addColumn(familyName, qualifierName);
        baseDao.delData(delete,Constant.TABLE_NAME);
    }

    public static void main(String[] args) throws Exception {
        StudentsServiceImpl ssi = new StudentsServiceImpl();
        //创建table
        ssi.createStuTable();
        //添加数据
        //列名/值
        Map<byte[],byte[]> bytes1 = new HashMap<byte[],byte[]>();
        bytes1.put(Constant.CLOUMN1,Bytes.toBytes("Jack"));
        bytes1.put(Constant.CLOUMN2,Bytes.toBytes("10"));
        bytes1.put(Constant.CLOUMN3,Bytes.toBytes("O:90,T:89,S:100"));
        //添加列族名称--列族名称,<列名,值>
        ssi.putStuData("cf1",bytes1);

        Map<byte[],byte[]> bytes2 = new HashMap<byte[],byte[]>();
        bytes2.put(Constant.CLOUMN1,Bytes.toBytes("Jack"));
        bytes2.put(Constant.CLOUMN2,Bytes.toBytes("10"));
        bytes2.put(Constant.CLOUMN3,Bytes.toBytes("O:90,T:89,S:100"));
        ssi.putStuData("cf2",bytes2);

        //查看数据
        Map<byte[],byte[]> byteScans = new HashMap<byte[], byte[]>();
        ResultScanner results = ssi.scanData(byteScans);
        for (Result result : results) {
            while (result.advance()) {
                System.out.println(result.current());
            }
        }
    }

}
