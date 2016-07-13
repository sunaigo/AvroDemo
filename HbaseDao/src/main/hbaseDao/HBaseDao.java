package main.hbaseDao;

import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Map;

/**
 * 
 * @author sunyulong
 *	
 */

public interface HBaseDao {
	
	//创建表
	public boolean createTable(String tableName, String[] columnFamily);
	
	//添加数据
	public boolean addData(String tableName , String rowKey , String columnFamily ,Map<String, String> data);
	
	//查询指定rowkey的所有数据
	public Result search (String rowKey , String tableName) throws IOException;
	
	//scan整个表
	public Result getResultScann(String tableName);
	
	//查询指定列族的某一字段的所有数据
	public Result getResultByColumn(String tableName, String rowKey,String columnFamily, String columnName);
	
	//更新一条数据
	public boolean update(String tableName, String rowKey,String columnFamily, String columnName, String value);
	
	//查询制定列的多个版本
	public Result getResultByVersion(String tableName, String rowKey,String columnFamily, String columnName);
	
	//删除指定列
	public boolean deleteColumn(String tableName, String rowKey,String columnFamily, String columnName);
	
	//删除指定rowKey的所有数据
	public boolean deleteAllColumn(String tableName, String rowKey);
	
	//删除表
	public boolean deleteTable(String tableName);
	
}
