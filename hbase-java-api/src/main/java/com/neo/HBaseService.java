package com.neo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseService {

	public static Configuration conf;

	static {
		conf =  HBaseConfiguration.create();
		conf.addResource("hbase-site.xml");
//		conf.set("hbase.zookeeper.quorum", "192.168.0.71");
//		conf.set("hbase.zookeeper.property.clientPort", "2181");
//		configuration.set("hbase.zookeeper.quorum", "study-90:2181,study-91:2181,study-92:2181");
	}

	/**
	 * 创建表
	 * 
	 * @param tablename 表名
	 * @param columnFamilys 列族
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	public static void createTable(String tablename, String[] columnFamilys)
			throws MasterNotRunningException, IOException, ZooKeeperConnectionException {
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		try {
			if (admin.tableExists(TableName.valueOf(tablename))) {
				System.out.println(tablename + " already exists");
			} else {
				TableName tableName = TableName.valueOf(tablename);
				HTableDescriptor tableDesc = new HTableDescriptor(tableName);

				// 在描述里添加列族
				for (String columnFamily : columnFamilys) {
					tableDesc.addFamily(new HColumnDescriptor(columnFamily));
				}
				admin.createTable(tableDesc);
				System.out.println(tablename + " created succeed");
			}
		} finally {
			admin.close();
			conn.close();
		}
	}
	
	/**
	 * 向表中插入一条新数据
	 * 
	 * @param tableName 表名
	 * @param row 行键key
	 * @param columnFamily 列族
	 * @param column 列名
	 * @param data 要插入的数据
	 * @throws IOException
	 */
	public static void putData(String tableName, String row, String columnFamily, String column, String data)
			throws IOException {
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf(tableName));
		try {
			Put put = new Put(Bytes.toBytes(row));
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
			table.put(put);
//			System.out.println("put '" + row + "','" + columnFamily + ":" + column + "','" + data + "'");
		} finally {
			table.close();
			conn.close();
		}
	}
	
	/**
	 * add a column family to an existing table
	 * 
	 * @param tableName table name 
	 * @param columnFamily column family
	 * @throws IOException
	 */
	public static void putFamily(String tableName, String columnFamily) throws IOException {
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		try {
			if (!admin.tableExists(TableName.valueOf(tableName))) {
				System.out.println(tableName + " not exists");
			} else {
				admin.disableTable(TableName.valueOf(tableName));
				
				HColumnDescriptor cf1 = new HColumnDescriptor(columnFamily);
				admin.addColumn(TableName.valueOf(tableName), cf1);
				
				admin.enableTable(TableName.valueOf(tableName));
				System.out.println(TableName.valueOf(tableName) + ", " + columnFamily + " put succeed");
			}
		} finally {
			admin.close();
			conn.close();
		}
	}


	/**
	 * 获取一条数据
	 * @param tableName
	 * @param rowKey
	 * @throws Exception
     */
	public static void getRow(String tableName, String rowKey) throws Exception {

		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf(tableName));
		try {
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = table.get(get);

			List<Cell> cells = result.listCells();
			for(Cell cell : cells){
				String row = new String(result.getRow(), "UTF-8");
				String family = new String(CellUtil.cloneFamily(cell), "UTF-8");
				String qualifier = new String(CellUtil.cloneQualifier(cell), "UTF-8");
				String value = new String(CellUtil.cloneValue(cell), "UTF-8");
				System.out.println("[row:"+row+"],[family:"+family+"],[qualifier:"+qualifier+"],[value:"+value+"]");
			}

		} finally {
			table.close();
			conn.close();
		}

	}

	
	/**
	 * 根据key读取一条数据
	 * 
	 * @param tableName 表名
	 * @param row 行键key
	 * @param columnFamily 列族
	 * @param column 列名
	 * @throws IOException
	 */
	public static void getData(String tableName, String row, String columnFamily, String column) throws IOException{
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf(tableName));
		try{
			Get get = new Get(Bytes.toBytes(row));
			Result result = table.get(get);
			byte[] rb = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
			String value = new String(rb, "UTF-8");
			System.out.println(value);
		} finally {
			table.close();
			conn.close();
		}
	}

	/**
	 * get all data of a table
	 * 
	 * @param tableName table name
	 * @throws IOException
	 */
	public static void scanAll(String tableName) throws IOException {
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf(tableName));
		try {
			Scan scan = new Scan();
			ResultScanner resultScanner = table.getScanner(scan);
			for(Result result : resultScanner){
				List<Cell> cells = result.listCells();
				for(Cell cell : cells){
					String row = new String(result.getRow(), "UTF-8");
					String family = new String(CellUtil.cloneFamily(cell), "UTF-8");
					String qualifier = new String(CellUtil.cloneQualifier(cell), "UTF-8");
					String value = new String(CellUtil.cloneValue(cell), "UTF-8");
					System.out.println("[row:"+row+"],[family:"+family+"],[qualifier:"+qualifier+"],[value:"+value+"]");
				}
			}
		} finally {
			table.close();
			conn.close();
		}
	}
	
	/**
	 * delete a data by row key
	 * 
	 * @param tableName table name
	 * @param rowKey row key
	 * @throws IOException
	 */
	public static void deleteRow(String tableName, String rowKey) throws IOException {
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf(tableName));
		try {
			List<Delete> list = new ArrayList<Delete>();
			Delete del = new Delete(rowKey.getBytes());
			list.add(del);
			table.delete(list);
			System.out.println("delete record " + rowKey + " ok");
		} finally {
			table.close();
			conn.close();
		}
	}

	/**
	 * 删除多条数据
	 * @param tableName
	 * @param rowKeys
	 * @throws Exception
     */
	public static void delMultiRows(String tableName, String[] rowKeys)
			throws Exception {
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf(tableName));
		try {
			List<Delete> delList = new ArrayList<Delete>();
			for (String rowKey : rowKeys) {
				Delete del = new Delete(Bytes.toBytes(rowKey));
				delList.add(del);
			}
			table.delete(delList);
			System.out.println("delete record " + rowKeys.toString() + " ok");
		} finally {
			table.close();
			conn.close();
		}
	}
	
	/**
	 * delete a column's value of a row
	 * 
	 * @param tableName table name
	 * @param rowKey row key
	 * @param familyName family name
	 * @param columnName column name
	 * @throws IOException
	 */
	public static void deleteColumn(String tableName, String rowKey, String familyName, String columnName) throws IOException {
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf(tableName));
		try{
			Delete del = new Delete(Bytes.toBytes(rowKey));
			del.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
			List<Delete> list = new ArrayList<Delete>(1);
			list.add(del);
			table.delete(list);
			System.out.println("[table:"+tableName+"],row:"+rowKey+"],[family:"+familyName+"],[qualifier:"+columnName+"]");
		} finally {
			table.close();
			conn.close();
		}
	}

	/**
	 * delete a columnFamily's all columns value of a row 
	 * 
	 * @param tableName table name
	 * @param rowKey row key
	 * @param familyName family name
	 * @throws IOException
	 */
	public static void deleteFamily(String tableName, String rowKey, String familyName) throws IOException {
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf(tableName));
		try{
			Delete del = new Delete(Bytes.toBytes(rowKey));
			del.addFamily(Bytes.toBytes(familyName));
			List<Delete> list = new ArrayList<Delete>(1);
			list.add(del);
			table.delete(list);
			System.out.println("[table:"+tableName+"],row:"+rowKey+"],[family:"+familyName+"]");
		} finally {
			table.close();
			conn.close();
		}
	}

	/**
	 * delete a table
	 * 
	 * @param tableName table name
	 * @throws IOException
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 */
	public static void dropTable(String tableName) throws IOException, MasterNotRunningException, ZooKeeperConnectionException {
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		try {
			admin.disableTable(TableName.valueOf(tableName));
			admin.deleteTable(TableName.valueOf(tableName));
			System.out.println("delete table " + tableName + " ok");
		} finally {
			admin.close();
			conn.close();
		}
	}


	/**
	 * 多条件查询
	 * @param tableName
	 * @param familyNames
	 * @param qualifiers
	 * @param values
     * @throws IOException
     */
	public static void queryByConditions(String tableName, String[] familyNames, String[] qualifiers,String[] values) throws IOException {
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf(tableName));
		try {
			List<Filter> filters = new ArrayList<Filter>();
			if (familyNames != null && familyNames.length > 0) {
				int i = 0;
				for (String familyName : familyNames) {
					Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(qualifiers[i]), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(values[i]));
					filters.add(filter);
					i++;
				}
			}
			FilterList filterList = new FilterList(filters);
			Scan scan = new Scan();
			scan.setFilter(filterList);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (Cell keyValue : r.rawCells()) {
					System.out.println("列：" + new String(CellUtil.cloneFamily(keyValue))+":"+new String(CellUtil.cloneQualifier(keyValue)) + "====值:" + new String(CellUtil.cloneValue(keyValue)));
				}
			}
			rs.close();
		} catch (Exception e) {
			table.close();
			conn.close();
		}

	}




}
