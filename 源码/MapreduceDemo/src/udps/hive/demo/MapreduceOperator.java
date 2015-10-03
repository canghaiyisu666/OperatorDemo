package udps.hive.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.thrift.TException;

public class MapreduceOperator extends Configured implements Tool {
	// 这种继承关系是为了得到提交脚本中的-D参数
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MapreduceOperator(),
				args);
		System.exit(res);

	}

	@Override
	public int run(String[] arg) throws Exception {
		Configuration conf = this.getConf();

		String jobinstanceid = "";
		String dbName = "";
		String inputTabName = "";
		String tempDatabaseName = "";
		String hdfsOutXml = "";
		String tempHdfsBasePath = "";
		String operFieldName = "";

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
		operFieldName = list.get(0).get("fieldName").toString();
		tempHdfsBasePath = list.get(0).get("tempHdfsBasePath").toString();

		conf.setStrings("field", operFieldName);
		Job job = Job.getInstance(conf);
		job.setJobName("MapreduceDemo");

		// 根据输入表给job初始化HCatInputFormat
		HCatInputFormat.setInput(job, dbName, inputTabName);
		job.setInputFormatClass(HCatInputFormat.class);

		job.setJarByClass(MapreduceOperator.class);
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(WritableComparable.class);
		job.setOutputValueClass(DefaultHCatRecord.class);

		// 用hcatalog创建输出表
		List<HCatFieldSchema> fieldSchemas = new ArrayList<HCatFieldSchema>();
		fieldSchemas.add(new HCatFieldSchema(operFieldName,
				TypeInfoFactory.stringTypeInfo, ""));
		HCatSchema s = new HCatSchema(fieldSchemas);// 构建输出表的表结构

		String tempOutPutTable = "t_" + jobinstanceid + "_"
				+ UUID.randomUUID().toString().replace('-', '_');// 根据规则生成输出表名
		createTable(tempDatabaseName, tempOutPutTable, s);// 自定义的方法用hcatalog创建输出表

		// 给job设置HCatOutputFormat相关参数
		OutputJobInfo outputJobInfo = OutputJobInfo.create(tempDatabaseName,
				tempOutPutTable, null);
		HCatOutputFormat.setOutput(job, outputJobInfo);
		HCatOutputFormat.setSchema(job, s);
		job.setOutputFormatClass(HCatOutputFormat.class);

		boolean result = job.waitForCompletion(true);// 提交作业并等待结果

		// 定义要生成xml文件的参数集合，准备生成stdout.xml文件
		List<Map> listOut = new ArrayList<Map>();
		Map<String, String> mapOut = new HashMap<String, String>();
		mapOut.put("jobinstanceid", jobinstanceid);
		if (result) { // 创建正常输出xml文件

			mapOut.put("tempDatabaseName", tempDatabaseName); // 设置正常输出xml文件参数
			mapOut.put("tempOutPutTable", tempOutPutTable);

			listOut.add(mapOut);
			if (tempHdfsBasePath.endsWith("/")) {
				hdfsOutXml = tempHdfsBasePath + "stdout.xml";
			} else {
				hdfsOutXml = tempHdfsBasePath + "/stdout.xml";
			}
			operXML.genStdoutXml(hdfsOutXml, listOut);

		} else { // 创建错误输出xml文件
			String errorMessage = "MR算子执行过程中出错。";
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
		}

		return 0;
	}

	@SuppressWarnings("rawtypes")
	public static class MyMap extends
			Mapper<WritableComparable, HCatRecord, LongWritable, Text> {
		String parm = ""; // 算子要处理的字段名（动态得到）

		@Override
		protected void setup(
				Mapper<WritableComparable, HCatRecord, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// 在执行map前从全局配置获取配置参数，得到要处理的字段名
			Configuration conf = context.getConfiguration();
			parm = conf.get("field");
			super.setup(context);
		}

		@Override
		protected void map(WritableComparable key, HCatRecord value,
				Context context) throws IOException, InterruptedException {
			// 从作业中获取表结构对象
			HCatSchema schema = HCatInputFormat.getTableSchema(context
					.getConfiguration());
			// 根据输入参数，对于每一行记录，获取需要的字段
			String field = value.getString(parm, schema);
			context.write((LongWritable) key, new Text(field));
		}
	}

	@SuppressWarnings("rawtypes")
	public static class MyReduce extends
			Reducer<WritableComparable, Text, WritableComparable, HCatRecord> {
		@Override
		protected void reduce(WritableComparable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			while (iter.hasNext()) {
				HCatRecord record = new DefaultHCatRecord(1);// 构造一行记录（1列）
				record.set(0, iter.next().toString().toUpperCase());// 给这行记录的第0个字段赋值
				context.write(null, record);
			}

		}
	}

	/**
	 * 通过hcatalog的schema在hive中的建表方法，使用RC存储，表已存在则先删除。
	 * 
	 * @param dbName
	 *            数据库名
	 * @param tblName
	 *            表名
	 * @param schema
	 *            表结构
	 */
	public static void createTable(String dbName, String tblName,
			HCatSchema schema) {
		HiveMetaStoreClient client = null;
		try {
			HiveConf hiveConf = HCatUtil.getHiveConf(new Configuration());
			try {
				client = HCatUtil.getHiveClient(hiveConf);
			} catch (MetaException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			if (client.tableExists(dbName, tblName)) {
				client.dropTable(dbName, tblName);
			}
		} catch (TException e) {
			e.printStackTrace();
		}

		List<FieldSchema> fields = HCatUtil.getFieldSchemaList(schema
				.getFields());
		System.out.println(fields);
		Table table = new Table();
		table.setDbName(dbName);
		table.setTableName(tblName);

		StorageDescriptor sd = new StorageDescriptor();
		sd.setCols(fields);
		table.setSd(sd);
		sd.setInputFormat(RCFileInputFormat.class.getName());
		sd.setOutputFormat(RCFileOutputFormat.class.getName());
		sd.setParameters(new HashMap<String, String>());
		sd.setSerdeInfo(new SerDeInfo());
		sd.getSerdeInfo().setName(table.getTableName());
		sd.getSerdeInfo().setParameters(new HashMap<String, String>());
		sd.getSerdeInfo().getParameters()
				.put(serdeConstants.SERIALIZATION_FORMAT, "1");
		sd.getSerdeInfo().setSerializationLib(
				org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class
						.getName());
		Map<String, String> tableParams = new HashMap<String, String>();
		table.setParameters(tableParams);
		try {
			client.createTable(table);
			System.out.println("Create table successfully!");
		} catch (TException e) {
			e.printStackTrace();
			return;
		} finally {
			client.close();
		}
	}
}
