package test;

import java.util.HashMap;

import main.hbaseDao.HBaseDao;
import main.hbaseDao.HBaseDaoImpl;

/**
 * 
 * @author sunyulong
 *
 */

public class TestHBase {

	public static void main(String[] args) {
		HBaseDao hd = new HBaseDaoImpl();
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("addr", "乌干达");
		hd.addData("t1", "000000", "cf1", map);
	}

}
