package main.hbaseDao;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;


/**
 * 
 * @author sunyulong
 *
 */

public class HBaseDaoImpl extends HBasePropreties implements HBaseDao{
	
	static Logger logger = Logger.getLogger(HBaseDaoImpl.class);
	static Configuration conf = null;
	static Connection conn = null;
	static HBaseAdmin HBaseAdmin = null;
	static {
		InputStream is = null;
		try {
			logger.info("正在加载配置文件...");
			is = new BufferedInputStream(new FileInputStream(new File("configration/HBaseConf.properties")));
			Properties properties = new Properties();  
			properties.load(is);
			String zoo = properties.getProperty(hbase_zookeeper_quorum);
			String port = properties.getProperty(hbase_zookeeper_property_clientPort);
			if (port == null || port.equals("")) {
				port = "2181";
			}
			conf = HBaseConfiguration.create();
			conf.set(hbase_zookeeper_quorum, zoo);
			conf.set(hbase_zookeeper_property_clientPort, port);
			conn = ConnectionFactory.createConnection(conf);
			logger.info("成功加载配置文件，正在初始化HBaseAdmin...");
			HBaseAdmin = (HBaseAdmin) conn.getAdmin();
			logger.info("初始化HBaseAdmin完成！");
		} catch (FileNotFoundException e) {
			logger.error("未找到配置文件！");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean createTable(String tableName, String[] columnFamily) {
		
		try{			
			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
			//添加列簇
			for (int i = 0; i < columnFamily.length; i++) {
				desc.addFamily(new HColumnDescriptor(columnFamily[i]));
			}
			if(HBaseAdmin.tableExists(tableName)){
				logger.error(tableName+"表已存在！");
				return false;
			}else{
				HBaseAdmin.createTable(desc);
				logger.info("成功创建表"+tableName+"！");
				return true;
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean addData(String tableName, String rowKey, String columnFamily,Map<String, String> data) {
		try {
			HTable table = (HTable)conn.getTable(TableName.valueOf(tableName));
			Put put = new Put(Bytes.toBytes(rowKey));
			Iterator<Map.Entry<String, String>> iterator = data.entrySet().iterator();
			while(iterator.hasNext()){
				Map.Entry<String, String> kv = iterator.next();
				put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(kv.getKey()), Bytes.toBytes(kv.getValue()));
			}
			table.put(put);
		} catch (TableNotFoundException e) {
			logger.error("未找到"+tableName+"表！");
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		logger.info("向表"+tableName+"插入了"+data.size()+"条数据！");
		return true;
	}

	@Override
	public Result search(String tableName, String rowKey){
		Result result = null;
		try {
			result = conn.getTable(TableName.valueOf(tableName)).get(new Get(Bytes.toBytes(rowKey)));
		} catch (IOException e) {			
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public ResultScanner getResultScann(String tableName) {
		ResultScanner rs = null;
		try {
			rs = conn.getTable(TableName.valueOf(tableName)).getScanner(new Scan());			
		} catch (IOException e) {			
			e.printStackTrace();
		}		
		return rs;

	}
	
	@Override
	public Result getResultByColumnFamily(String tableName, String rowKey, String columnFamily) {
		Result result = null;
		try {
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addFamily(Bytes.toBytes(columnFamily));
			result = conn.getTable(TableName.valueOf(tableName)).get(get);
		} catch (IOException e) {			
			e.printStackTrace();
		}
		return result;
	}
	
	@Override
	public Result getResultByColumn(String tableName, String rowKey, String columnFamily, String columnName) {
		Result result = null;
		try {
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
			result = conn.getTable(TableName.valueOf(tableName)).get(get);
		} catch (IOException e) {			
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public boolean update(String tableName, String rowKey, String columnFamily, String columnName, String value) {
		try {
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
			conn.getTable(TableName.valueOf(tableName)).put(put);			
		} catch (IOException e) {			
			e.printStackTrace();
			return false;
		}	
		logger.info(tableName+"的"+columnFamily+"列族的"+columnName+"列已被更新！");
		return true;
	}

	@Override
	public Result getResultByVersion(String tableName, String rowKey, String columnFamily, String columnName) {
		Result result = null;
		try {
			Get get = new Get(Bytes.toBytes(rowKey));
			get.setMaxVersions();
			get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));			
			result = conn.getTable(TableName.valueOf(tableName)).get(get);
		} catch (IOException e) {			
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public boolean deleteColumn(String tableName, String rowKey, String columnFamily, String columnName) {
		Delete d = new Delete(Bytes.toBytes(rowKey));
		d.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
		try {
			conn.getTable(TableName.valueOf(tableName)).delete(d);
		} catch (IOException e1) {
			e1.printStackTrace();
			return false;
		}
		logger.info(tableName+"的"+columnFamily+"列族的"+columnName+"列已被删除！");
		return true;
	}

	@Override
	public boolean deleteColumnFamily(String tableName, String columnFamily) {
		try {
			HBaseAdmin.deleteColumn(tableName, columnFamily);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}		
		logger.info(tableName+"的"+columnFamily+"列族已被删除！");
		return true;

	}

	@Override
	public boolean deleteTable(String tableName) {
		try {
			logger.info("正在删除表"+tableName+"......");
			HBaseAdmin.disableTable(tableName);
			HBaseAdmin.deleteTable(tableName);
			logger.info(tableName+"表已删除!");
		} catch (MasterNotRunningException e) {
			logger.error("HMaster未运行！");
			e.printStackTrace();
			return false;
		} catch (ZooKeeperConnectionException e) {
			logger.error("未能连接到zookeeper！");
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			logger.error("删除表时发生读写异常！");
			e.printStackTrace();
			return false;
		}		
		return true;
	}

}
