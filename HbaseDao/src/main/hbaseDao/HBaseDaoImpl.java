package main.hbaseDao;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

/**
 * 
 * @author sunyulong
 *
 */

public class HBaseDaoImpl implements HBaseDao {
	
	static Logger logger = Logger.getLogger(HBaseDaoImpl.class);
	static Configuration conf = null;
	static {
		InputStream is = null;
		try {
			is = new BufferedInputStream(new FileInputStream(new File("conf/HBaseConf.properties")));
			Properties properties = new Properties();  
			properties.load(is);
			String zoo = properties.getProperty("hbase.zookeeper.quorum");
			String port = properties.getProperty("hbase.zookeeper.property.clientPort");
			if (port == null || port.equals("")) {
				port = "2181";
			}
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", zoo);
			conf.set("hbase.zookeeper.property.clientPort", port);
		} catch (FileNotFoundException e) {
			logger.error("未找到配置文件！");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean createTable(String tableName, String[] columnFamily) {
		Connection conn = null;
		try{
			conn = ConnectionFactory.createConnection(conf);
			HBaseAdmin hBaseAdmin = (HBaseAdmin) conn.getAdmin();
			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
			//添加列簇
			for (int i = 0; i < columnFamily.length; i++) {
				desc.addFamily(new HColumnDescriptor(columnFamily[i]));
			}
			if(hBaseAdmin.tableExists("test")){
				logger.info("表已存在！");
				return false;
			}else{
				hBaseAdmin.createTable(desc);
				logger.info("成功创建表！");
				return true;
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(null != conn){
				try {
					conn.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return false;
		
		
	
		
		
		/*HBaseAdmin admin;
		try {
			Connection con = ConnectionFactory.createConnection(conf);
			
			admin = new HBaseAdmin(conf);
			HTableDescriptor desc = new HTableDescriptor(tableName);
			for (int i = 0; i < columnFamily.length; i++) {
				desc.addFamily(new HColumnDescriptor(columnFamily[i]));
			}
			if (admin.tableExists(tableName)) {
				logger.error("表已存在！");
				admin.close();
				return false;
			} else {
				admin.createTable(desc);
				logger.info("成功创建表！");
				admin.close();
				return true;
			}
		} catch (MasterNotRunningException e) {
			logger.error("Hmaster未运行");
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			logger.error("未能连接到Zookeeper");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
			return false;*/
	}

	@Override
	public boolean addData(String tableName, String rowKey, String columnFamily, String[] cf, String[] data) {

		return false;
	}

	@Override
	public boolean addData(String tableName, String rowKey, String columnFamily, String[] cf1, String[] data1,
			String[] cf2, String[] data2) {

		return false;
	}

	@Override
	public Result search(String rowKey, String tableName) {

		return null;
	}

	@Override
	public Result getResultScann(String tableName) {
		return null;

	}

	@Override
	public Result getResultByColumn(String tableName, String rowKey, String columnFamily, String columnName) {

		return null;
	}

	@Override
	public boolean update(String tableName, String rowKey, String columnFamily, String columnName, String value) {

		return false;
	}

	@Override
	public Result getResultByVersion(String tableName, String rowKey, String columnFamily, String columnName) {
		return null;

	}

	@Override
	public boolean deleteColumn(String tableName, String rowKey, String columnFamily, String columnName) {
		return false;

	}

	@Override
	public boolean deleteAllColumn(String tableName, String rowKey) {
		return false;

	}

	@Override
	public boolean deleteTable(String tableName) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println(tableName + "is deleted!");
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		return false;
	}

}
