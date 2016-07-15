package main.hbaseDao;

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
	
	private static Logger logger = Logger.getLogger(HBaseDaoImpl.class);
	private static Configuration conf = new Configuration();
	private static Connection conn = null;
	private static HBaseAdmin HBaseAdmin = null;
	static {
		try {
			if (logger.isDebugEnabled())
			    logger.debug("starting to load config file...");
			InputStream is = ClassLoader.getSystemResourceAsStream("configration/HBaseConf.properties");
			Properties properties = new Properties();
			properties.load(is);
			String zoo = properties.getProperty(HBASE_ZOOKEEPER_QUORUM);
			String port = properties.getProperty(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT);
			if (port == null || port.equals("")) {
				port = "2181";
			}
			conf = HBaseConfiguration.create();
			conf.set(HBASE_ZOOKEEPER_QUORUM, zoo);
			conf.set(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, port);
			conn = ConnectionFactory.createConnection(conf);
            if (logger.isDebugEnabled())
			    logger.debug("Load config success ! starting to init HBaseAdmin...");
			HBaseAdmin = (HBaseAdmin) conn.getAdmin();
            if (logger.isDebugEnabled())
			    logger.debug("init HbaseAdmin success!");
		} catch (FileNotFoundException e) {
			logger.error("config not found!");
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
			for (String cf : columnFamily) {
				desc.addFamily(new HColumnDescriptor(cf));
			}
			if(HBaseAdmin.tableExists(tableName)){
				logger.error("table "+tableName+" exits！");
				return false;
			}else{
				HBaseAdmin.createTable(desc);
                if (logger.isDebugEnabled())
				    logger.debug("create table "+tableName+" success！");
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
			logger.error("table "+tableName+" net found!");
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
        if (logger.isDebugEnabled())
		    logger.debug("insert into "+tableName+" data num "+data.size());
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

        if (logger.isDebugEnabled())
		    logger.info(tableName+"'s "+columnFamily+" columnFamily "+columnName+" be updated!");
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
        if (logger.isDebugEnabled())
		    logger.debug(tableName+"'s "+columnFamily+"'s "+columnName+" is deleted!");
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
        if (logger.isDebugEnabled())
		    logger.debug(tableName+"'s "+columnFamily+" is deleted!");
		return true;

	}

	@Override
	public boolean deleteTable(String tableName) {
		try {
            if (logger.isDebugEnabled())
			    logger.debug("starting to delete "+tableName+"......");
			HBaseAdmin.disableTable(tableName);
			HBaseAdmin.deleteTable(tableName);
            if (logger.isDebugEnabled())
			    logger.debug(tableName+"is deleted!");
		} catch (MasterNotRunningException e) {
			logger.error("HMaster not run！");
			e.printStackTrace();
			return false;
		} catch (ZooKeeperConnectionException e) {
			logger.error("can't connected with zookeeper！");
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}		
		return true;
	}

}
