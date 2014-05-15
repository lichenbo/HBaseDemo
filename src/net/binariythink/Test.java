package net.binariythink;

import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {
	private static Configuration conf = null;
	
	static {
		conf = HBaseConfiguration.create();
	}
	
	public static void createTable(String tableName,String[] familys) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("table already exists");
		} else {
			HTableDescriptor ht = new HTableDescriptor(tableName);
			for (String family: familys) {
				ht.addFamily(new HColumnDescriptor(family));
			}
			admin.createTable(ht);
			System.out.println("Table created OK");
		}
	}
	
	public static void insertData(String tableName, String[] familys, Map<String,Integer> datas) throws Exception {
		HTable table = new HTable(conf,tableName);
		List<Put> lp = new ArrayList<Put>(datas.size());
		for (Map.Entry<String, Integer> data: datas.entrySet()) {
			Put p = new Put(Bytes.toBytes(data.getKey()));
			p.add(Bytes.toBytes(familys[0]), Bytes.toBytes("data"), Bytes.toBytes(data.getKey()));
			p.add(Bytes.toBytes(familys[1]), Bytes.toBytes("data"), Bytes.toBytes(data.getValue()));
			lp.add(p);
		}
		table.put(lp);
		System.out.println("Data inserted.");
	}
	
	public static void main(String[] args) throws Exception {
		createTable("Shakespeare",new String[]{"word","count"});
		Scanner scanner = new Scanner(new FileReader("/home/hadoop/workspace/HBaseHive/input.txt"));
		Map<String,Integer> m = new HashMap<String,Integer>();
		PrintWriter fp = new PrintWriter("output.txt");
		while(scanner.hasNextLine()) {
			String line = scanner.nextLine();
			System.out.println(line);
			String[] keyValue = line.split("\t");
			int count = 0;
			System.out.println(keyValue[0]);
			for (String value: keyValue[1].split(";")) {
				count += Integer.parseInt(value.split("#")[1]);
			}
			m.put(keyValue[0], count);
			
			fp.write(keyValue[0] + "\t" + count + "\n");
		}
		scanner.close();
		fp.close();
		insertData("Shakespeare", new String[]{"word","count"},m);
	}
}
