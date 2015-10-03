package udps.hive.demo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HiveOperator extends Configured implements Tool {
	// 这种继承关系是为了得到提交脚本中的-D参数

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new HiveOperator(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] arg) throws Exception {
		Configuration conf = this.getConf();
		String[] queue = conf.getStrings("mapreduce.job.queuename");// 获取提交脚本中的队列参数

		String jobinstanceid = "";
		String dbName = "";
		String inputTabName = "";
		String operFieldName = "";
		String tempDatabaseName = "";
		String hdfsOutXml = "";
		String hiveserver2ip = "";
		String tempHdfsBasePath = "";

		// 读取stdin.xml文件
		String stdinXml = arg[1];
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream dis = fs.open(new Path(stdinXml));
		InputStreamReader isr = new InputStreamReader(dis, "utf-8");
		BufferedReader read = new BufferedReader(isr);
		String tempString = "";
		String xmlParams = "";
		while ((tempString = read.readLine()) != null) {
			xmlParams += "\n" + tempString;
		}
		read.close();
		xmlParams = xmlParams.substring(1);

		// 获取stdin.xml文件中的参数值
		OperatorParamXml operXML = new OperatorParamXml();
		List<Map> list = operXML.parseStdinXml(xmlParams);
		dbName = list.get(0).get("dbName").toString();
		inputTabName = list.get(0).get("inputTabName").toString();
		jobinstanceid = list.get(0).get("jobinstanceid").toString();
		tempDatabaseName = list.get(0).get("tempDatabaseName").toString();
		hiveserver2ip = list.get(0).get("hiveserver2ip").toString();
		operFieldName = list.get(0).get("fieldName").toString();
		tempHdfsBasePath = list.get(0).get("tempHdfsBasePath").toString();

		// 加载UDPS提供的jdbc驱动。在本地环境调试时可先使用原生的驱动，保证程序逻辑正常后再更改驱动名。
		Class.forName("com.udps.hive.jdbc.HiveDriver");

		String connectionUrl = "jdbc:hive2://" + hiveserver2ip
				+ "/;auth=noSasl";
		System.out.println("connectionUrl==>" + connectionUrl);
		Connection con = DriverManager.getConnection(connectionUrl);// 建立与hiveserver2的连接

		Statement stmt = con.createStatement();

		String sql1 = "set mapreduce.job.queuename=" + queue[0];

		String tempOutPutTable = "t_" + jobinstanceid + "_"
				+ UUID.randomUUID().toString().replace('-', '_');// 根据规则生成输出表的表名
		String outPutTable = tempDatabaseName + "." + tempOutPutTable;
		String sql2 = "create table " + outPutTable + "(test string)";// 创建输出表
		String sql3 = "insert into table " + outPutTable + " select upper("
				+ operFieldName + ") from " + dbName + "." + inputTabName;// 核心分析语句

		// 定义要生成stdout文件的参数集合
		List<Map> listOut = new ArrayList<Map>();
		Map<String, String> mapOut = new HashMap<String, String>();
		mapOut.put("jobinstanceid", jobinstanceid);

		try {
			stmt.execute(sql1);// 设置hive在yarn执行时的队列
			stmt.execute(sql2);// 创建临时输出表
			stmt.execute(sql3);// 执行分析任务，并将结果保存到输出表
		} catch (Exception e) {
			// 创建错误输出xml文件
			String errorMessage = "执行sql过程中出错。";
			System.out.println("=====errorMessage=====" + errorMessage);
			String errotCode = "80001";
			mapOut.put("errorMessage", errorMessage);
			mapOut.put("errotCode", errotCode);
			listOut.add(mapOut);
			if (tempHdfsBasePath.endsWith("/")) {
				hdfsOutXml = tempHdfsBasePath + "stderr.xml";
			} else {
				hdfsOutXml = tempHdfsBasePath + "/stderr.xml";
			}
			operXML.genStderrXml(hdfsOutXml, listOut);

			return 0;
		}

		// 创建正常输出xml文件，设置正常输出xml文件参数
		mapOut.put("tempDatabaseName", tempDatabaseName);
		mapOut.put("tempOutPutTable", tempOutPutTable);
		listOut.add(mapOut);
		if (tempHdfsBasePath.endsWith("/")) {
			hdfsOutXml = tempHdfsBasePath + "stdout.xml";
		} else {
			hdfsOutXml = tempHdfsBasePath + "/stdout.xml";
		}
		operXML.genStdoutXml(hdfsOutXml, listOut);

		return 0;
	}
}
