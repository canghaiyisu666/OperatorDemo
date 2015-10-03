package iie.udps.example.spark;

import iie.udps.common.hcatalog.SerHCatInputFormat;
import iie.udps.common.hcatalog.SerHCatOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.thrift.TException;

import scala.Tuple2;

/**
 * spark+hcatalog 实现表的复制功能， 并将原表一列数据变成大写存到新表 ;
 * 
 */
public class LowerUpperCaseConvert {

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String jobinstanceid = null;
		String dbName = null;
		String inputTabName = null;
		String operFieldName = null;
		String tempDatabaseName = null;
		String tempHdfsBasePath = null;
		String hdfsOutXml = null;

		// 读取stdin.xml文件
		String stdinXml = args[1];
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
		tempHdfsBasePath = list.get(0).get("tempHdfsBasePath").toString();
		operFieldName = list.get(0).get("fieldName").toString();// 获取要操作的表字段名

		// 获得输入表的schema
		HiveConf hiveConf = HCatUtil.getHiveConf(new Configuration());
		HiveMetaStoreClient client = HCatUtil.getHiveClient(hiveConf);
		org.apache.hadoop.hive.ql.metadata.Table inputTable = HCatUtil
				.getTable(client, dbName, inputTabName);
		HCatSchema schema = HCatUtil.getTableSchemaWithPtnCols(inputTable);

		// 根据字段名获得字段位置和类型
		int position = schema.getPosition(operFieldName);
		String operFieldType = schema.get(operFieldName).getTypeString();

		// 定义要生成xml文件的参数集合
		List<Map> listOut = new ArrayList<Map>();
		Map<String, String> mapOut = new HashMap<String, String>();
		mapOut.put("jobinstanceid", jobinstanceid);

		// 判断如果stdin.xml文件中的操作参数值为空，直接输出错误信息到stderr.xml文件
		if (dbName == "" || inputTabName == "" || jobinstanceid == ""
				|| tempDatabaseName == "" || tempHdfsBasePath == ""
				|| operFieldName == "") {

			// 创建错误输出xml文件
			String errorMessage = "解析参数有误";
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
		} else {
			if (operFieldType.equalsIgnoreCase("string")) {
				String tempOutPutTable = "t_" + jobinstanceid + "_"
						+ UUID.randomUUID().toString().replace('-', '_');// 根据规则生成输出表的名字

				createTable(tempDatabaseName, tempOutPutTable, schema);// 用自定义的方法创建输出表

				// 通过spark 复制输入表数据到输出表，并将指定字段转化成大写
				JavaSparkContext jsc = new JavaSparkContext(
						new SparkConf().setAppName("SparkDemo"));
				JavaRDD<SerializableWritable<HCatRecord>> rdd1 = LowerUpperCaseConvert
						.lowerUpperCaseConvert(jsc, dbName, inputTabName,
								position);
				LowerUpperCaseConvert.storeToTable(rdd1, tempDatabaseName,
						tempOutPutTable);
				jsc.stop();

				// 创建正常输出xml文件，设置输出xml文件参数
				mapOut.put("tempDatabaseName", tempDatabaseName);
				mapOut.put("tempOutPutTable", tempOutPutTable);
				listOut.add(mapOut);
				if (tempHdfsBasePath.endsWith("/")) {
					hdfsOutXml = tempHdfsBasePath + "stdout.xml";
				} else {
					hdfsOutXml = tempHdfsBasePath + "/stdout.xml";
				}
				operXML.genStdoutXml(hdfsOutXml, listOut);
			} else {// 创建错误输出xml文件
				String errorMessage = "其他错误";
				String errotCode = "80002";
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
		}
		System.exit(0);
	}

	@SuppressWarnings("rawtypes")
	public static JavaRDD<SerializableWritable<HCatRecord>> lowerUpperCaseConvert(
			JavaSparkContext jsc, String dbName, String inputTabName,
			int position) throws IOException {

		Configuration inputConf = new Configuration();
		SerHCatInputFormat.setInput(inputConf, dbName, inputTabName);

		JavaPairRDD<WritableComparable, SerializableWritable> rdd = jsc
				.newAPIHadoopRDD(inputConf, SerHCatInputFormat.class,
						WritableComparable.class, SerializableWritable.class);// 通过SerHcatalog把输入表转为rdd

		final Broadcast<Integer> posBc = jsc.broadcast(position);
		// 获取表记录集
		JavaRDD<SerializableWritable<HCatRecord>> result = rdd
				.map(new Function<Tuple2<WritableComparable, SerializableWritable>, SerializableWritable<HCatRecord>>() {
					private static final long serialVersionUID = -2362812254158054659L;

					private final int postion = posBc.getValue().intValue();

					public SerializableWritable<HCatRecord> call(
							Tuple2<WritableComparable, SerializableWritable> v)
							throws Exception {
						HCatRecord record = (HCatRecord) v._2().value();
						record.set(postion, record.get(postion).toString()
								.toUpperCase());
						return new SerializableWritable<HCatRecord>(record);
					}
				});
		return result;
	}

	@SuppressWarnings("rawtypes")
	public static void storeToTable(
			JavaRDD<SerializableWritable<HCatRecord>> rdd, String dbName,
			String tblName) {
		Job outputJob = null;
		try {
			outputJob = Job.getInstance();
			outputJob.setJobName("SparkJob");
			outputJob.setOutputKeyClass(WritableComparable.class);
			outputJob.setOutputValueClass(SerializableWritable.class);

			SerHCatOutputFormat.setOutput(outputJob,
					OutputJobInfo.create(dbName, tblName, null));
			HCatSchema schema = SerHCatOutputFormat.getTableSchema(outputJob
					.getConfiguration());
			SerHCatOutputFormat.setSchema(outputJob, schema);
			outputJob.setOutputFormatClass(SerHCatOutputFormat.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// 将RDD存储到目标表中
		rdd.mapToPair(
				new PairFunction<SerializableWritable<HCatRecord>, WritableComparable, SerializableWritable<HCatRecord>>() {

					private static final long serialVersionUID = -4658431554556766962L;

					@Override
					public Tuple2<WritableComparable, SerializableWritable<HCatRecord>> call(
							SerializableWritable<HCatRecord> record)
							throws Exception {
						return new Tuple2<WritableComparable, SerializableWritable<HCatRecord>>(
								NullWritable.get(), record);
					}
				}).saveAsNewAPIHadoopDataset(outputJob.getConfiguration());
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
