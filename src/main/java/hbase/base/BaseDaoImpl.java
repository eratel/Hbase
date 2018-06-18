package hbase.base;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Klein
 * @Package hbase.students
 * @Description: students服务
 * @date 17/6/18 11:36
 */
public class BaseDaoImpl implements BaseDao {
    static Logger logger = LoggerFactory.getLogger(BaseDaoImpl.class);


    /**
     * 创建表
     * @param tableDescriptor
     */
    public void createTable(HTableDescriptor tableDescriptor) throws Exception{
        Admin admin = BaseConfig.getConnection().getAdmin();
        //判断tablename是否存在
        if (!admin.tableExists(tableDescriptor.getTableName())) {
            admin.createTable(tableDescriptor);
        }
        admin.close();
    }


    /**
     *  添加列族
     *  @param tableName
     *  @param columnDescriptor
     */
    public void addTableColumn(String tableName,HColumnDescriptor columnDescriptor) throws Exception {
        Admin admin = BaseConfig.getConnection().getAdmin();
        admin.addColumn(TableName.valueOf(tableName),columnDescriptor);
        admin.close();
    }


    /**
     *  新增数据
     * @param putData
     *  @param tableName
     */
    public void putData(Put putData,String tableName) throws Exception{
        Table table = BaseConfig.getConnection().getTable(TableName.valueOf(tableName));
        table.put(putData);
        table.close();
    }

    /**
     *  新增数据
     * @param putList
     */
    public void putData(List<Put> putList,String tableName) throws Exception{
        Table table = BaseConfig.getConnection().getTable(TableName.valueOf(tableName));
        table.put(putList);
        table.close();
    }

    /**
     * 删除数据
     * @param delData
     *  @param tableName
     */
    public void delData(Delete delData,String tableName) throws Exception{
        Table table = BaseConfig.getConnection().getTable(TableName.valueOf(tableName));
        table.delete(delData);
        table.close();
    }

    /**
     * 查询数据
     * @param scan
     *  @param tableName
     * @return
     */
    public ResultScanner scanData(Scan scan,String tableName) throws Exception{
        Table table = BaseConfig.getConnection().getTable(TableName.valueOf(tableName));
        ResultScanner rs =  table.getScanner(scan);
        table.close();
        return rs;
    }

    /**
     * 查询数据
     * @param get
     *  @param tableName
     * @return
     */
    public Result getData(Get get,String tableName) throws Exception{
        Table table = BaseConfig.getConnection().getTable(TableName.valueOf(tableName));
        Result result = table.get(get);
        table.close();
        return result;
    }
}
