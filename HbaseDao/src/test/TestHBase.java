package test;

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
		System.out.println(hd.createTable("t2", new String [] {"cf1","cf2"}));
	}

}
