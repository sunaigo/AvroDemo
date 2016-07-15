import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import main.hbaseDao.HBaseDao;
import main.hbaseDao.HBaseDaoImpl;

/**
 * 
 * @author sunyulong
 *
 */

public class TestHBase {

	public static void main(String[] args) throws IOException {
		HBaseDao hd = new HBaseDaoImpl();
		hd.createTable("t1", new String[]{"cf1","cf2"});
//		hd.deleteTable("t1");
		/*
		HashMap<String,String> cf1 = new HashMap<String, String>();
		cf1.put("name", "列宁");
		cf1.put("age", "22");
		cf1.put("tall", "181cm");
		hd.addData("t1", "000001", "cf1", cf1);
		
		HashMap<String,String> cf2 = new HashMap<String, String>();
		cf2.put("phone", "15925562301");
		cf2.put("addr", "几内亚");
		cf2.put("weapon", "政治书");
		hd.addData("t1", "000001", "cf2", cf2);
		*/
		
		/*
		Result rs = hd.search("t2", "000001");
		for(Cell cell : rs.rawCells()){
			System.out.println("列簇："+new String(CellUtil.cloneFamily(cell)));
			System.out.println("列名："+new String(CellUtil.cloneQualifier(cell)));
			System.out.println("参数："+new String(CellUtil.cloneValue(cell)));
		}
		*/
		
		/*
		ResultScanner rs = hd.getResultScann("t2");
		for (Result r : rs) {
            for (Cell cell : r.rawCells()) {            	
                System.out.println("row:" + new String(CellUtil.cloneRow(cell)));
                System.out.println("family:"+ new String(CellUtil.cloneFamily(cell)));
                System.out.println("qualifier:" + new String(CellUtil.cloneQualifier(cell)));
                System.out.println("value:" + new String(CellUtil.cloneValue(cell)));
                System.out.println("timestamp:" + cell.getTimestamp());
            }
		}
		*/
		/*
		Result rs = hd.getResultByColumn("t2", "000000", "cf1", "name");
		for(Cell cell : rs.rawCells()){
			System.out.println("列簇："+new String(CellUtil.cloneFamily(cell)));
			System.out.println("列名："+new String(CellUtil.cloneQualifier(cell)));
			System.out.println("参数："+new String(CellUtil.cloneValue(cell)));
		}
		*/
		/*
		Result rs = hd.getResultByColumnFamily("t2", "000000", "cf1");
		for(Cell cell : rs.rawCells()){
			System.out.println("列簇："+new String(CellUtil.cloneFamily(cell)));
			System.out.println("列名："+new String(CellUtil.cloneQualifier(cell)));
			System.out.println("参数："+new String(CellUtil.cloneValue(cell)));
		}
		*/
		//hd.update("t2", "000000", "cf2", "addr", "蜀");
		/*
		Result rs = hd.getResultByVersion("t2", "000000", "cf2","addr");
		for(Cell cell : rs.rawCells()){
			System.out.println("列簇："+new String(CellUtil.cloneFamily(cell)));
			System.out.println("列名："+new String(CellUtil.cloneQualifier(cell)));
			System.out.println("参数："+new String(CellUtil.cloneValue(cell)));
		}
		*/
		//hd.deleteColumn("t1", "000001", "cf2", "phone");
		
	}

}










