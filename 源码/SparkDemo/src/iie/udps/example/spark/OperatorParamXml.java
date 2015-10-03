package iie.udps.example.spark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

/**
 * Dom4j 生成XML文档与解析XML文档
 */
public class OperatorParamXml {

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws Exception {

		String userName = null;
		String operatorName = null;
		String dbName = null;
		String inputTabName = null;
		String jobinstanceid = null;
		ArrayList<String> fieldName = new ArrayList<String>();
		ArrayList<String> fieldType = new ArrayList<String>();
		int fieldCount = 0;

		File file = new File("F:/stdin.xml");
		BufferedReader read = new BufferedReader(new FileReader(file));
		String tempString = "";
		String xmlParams = "";
		while ((tempString = read.readLine()) != null) {
			xmlParams += "\n" + tempString;
		}
		read.close();
		xmlParams = xmlParams.substring(1);
		System.out.println(xmlParams + "\n``````````````````````");
		OperatorParamXml operXML = new OperatorParamXml();
		List<Map> list = operXML.parseStdinXml(xmlParams);
		jobinstanceid = list.get(0).get("jobinstanceid").toString();
		System.out.println("====jobinstanceid=====" + jobinstanceid);
		userName = list.get(0).get("userName").toString();
		System.out.println("====userName=====" + userName);
		dbName = list.get(0).get("dbName").toString();
		System.out.println("====dbName=====" + dbName);
		inputTabName = list.get(0).get("inputTabName").toString();
		System.out.println("====inputTabName=====" + inputTabName);
		operatorName = list.get(0).get("operatorName").toString();
		System.out.println("====operatorName=====" + operatorName);
		fieldCount = Integer.parseInt(list.get(0).get("fieldCount").toString());
		System.out.println("====fieldCount=====" + fieldCount);
		for (int i = 1; i <= fieldCount; i++) {
			fieldName.add(list.get(0).get("fieldName" + i).toString());
			System.out.println("====fieldname=====" + fieldName.get(i - 1));
			fieldType.add(list.get(0).get("fieldType" + i).toString());
			System.out.println("====fieldtype=====" + fieldType.get(i - 1));
		}

	}

	@SuppressWarnings("rawtypes")
	public List<Map> parseStdinXml(String xmlParams) throws Exception {

		String userName = null;
		String operatorName = null;
		String dbName = null;
		String inputTabName = null;
		String strs = null;
		String fieldName = null;
		String inputFilePath = null;
		String schemaList = null;
		String jobinstanceid = null;
		String tempDatabaseName = null;
		String tempHdfsBasePath = null;

		List<Map> list = new ArrayList<Map>();
		Map<String, String> map = new HashMap<String, String>();
		Document document = DocumentHelper.parseText(xmlParams); // 将字符串转化为xml
		Element node1 = document.getRootElement(); // 获得根节点
		Iterator iter1 = node1.elementIterator(); // 获取根节点下的子节点
		while (iter1.hasNext()) {
			Element node2 = (Element) iter1.next();
			// 获取jobinstanceid
			if ("jobinstanceid".equals(node2.getName())) {
				jobinstanceid = node2.getText();
				map.put("jobinstanceid", jobinstanceid);
				System.out.println("====jobinstanceid=====" + jobinstanceid);
			}
			// 获取通用参数
			if ("context".equals(node2.getName())) {
				Iterator iter2 = node2.elementIterator();
				while (iter2.hasNext()) {
					Element node3 = (Element) iter2.next();
					if ("property".equals(node3.getName())) {
						if ("userName".equals(node3.attributeValue("name"))) {
							userName = node3.attributeValue("value");
						} else if ("tempDatabaseName".equals(node3
								.attributeValue("name"))) {
							tempDatabaseName = node3.attributeValue("value");
						} else if ("tempHdfsBasePath".equals(node3
								.attributeValue("name"))) {
							tempHdfsBasePath = node3.attributeValue("value");
						}
					}
				}
				map.put("userName", userName);
				map.put("tempDatabaseName", tempDatabaseName);
				map.put("tempHdfsBasePath", tempHdfsBasePath);
			}
			// 获取算子参数
			if ("operator".equals(node2.getName())) {
				operatorName = node2.attributeValue("name");
				map.put("operatorName", operatorName);
				Iterator iter2 = node2.elementIterator();
				while (iter2.hasNext()) {
					Element node3 = (Element) iter2.next();
					if ("parameter".equals(node3.getName())) {
						if ("field1".equals(node3.attributeValue("name"))) {
							fieldName = node3.getText();
							map.put("fieldName", fieldName);
						}
						if ("inputFilePath".equals(node3.attributeValue("name"))) {
							inputFilePath = node3.getText();
							map.put("inputFilePath", inputFilePath);
						}
						if ("schemaList".equals(node3.attributeValue("name"))) {
							schemaList = node3.getText();
							map.put("schemaList", schemaList);
						}
					}
				}
			}

			// 获取输入数据库
			if ("datasets".equals(node2.getName())) {
				Iterator iter2 = node2.elementIterator();
				while (iter2.hasNext()) {
					Element node3 = (Element) iter2.next();
					if ("inport1".equals(node3.attributeValue("name"))) {
						Iterator iter3 = node3.elementIterator();
						while (iter3.hasNext()) {
							Element node4 = (Element) iter3.next();
							strs = node4.getText();
						}
						if (!"".equals(strs.trim())) {
							String[] arr = strs.split("\\.");
							dbName = arr[0];
							inputTabName = arr[1];
						}
					}
					map.put("dbName", dbName);
					map.put("inputTabName", inputTabName);
				}
			}
		}
		list.add(map);
		return list;
	}

	/* 生成stdout.Xml文件 */
	@SuppressWarnings("rawtypes")
	public void genStdoutXml(String fileName, List<Map> listOut) {

		String jobinstance = null;
		String tempDatabaseName = null;
		String tempOutPutTable = null;

		tempDatabaseName = listOut.get(0).get("tempDatabaseName").toString();
		jobinstance = listOut.get(0).get("jobinstanceid").toString();
		tempOutPutTable = listOut.get(0).get("tempOutPutTable").toString();

		Document document = DocumentHelper.createDocument();
		Element response = document.addElement("response");
		Element jobinstanceid = response.addElement("jobinstanceid");
		jobinstanceid.setText(jobinstance);
		Element datasets = response.addElement("datasets");
		Element dataset = datasets.addElement("dataset");
		dataset.addAttribute("name", "outport1");
		Element row = dataset.addElement("row");
		row.setText(tempDatabaseName + "." + tempOutPutTable);

		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(fileName), conf);
			OutputStream out = fs.create(new Path(fileName),
					new Progressable() {
						public void progress() {
						}
					});
			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("UTF-8");
			XMLWriter xmlWriter = new XMLWriter(out, format);
			xmlWriter.write(document);
			xmlWriter.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}

	}

	/* 生成stderr.xml文件 */
	@SuppressWarnings("rawtypes")
	public void genStderrXml(String fileName, List<Map> listOut) {

		String jobinstance = null;
		String errorMessage = null;
		String errotCode = null;
		jobinstance = listOut.get(0).get("jobinstanceid").toString();
		errorMessage = listOut.get(0).get("errorMessage").toString();
		errotCode = listOut.get(0).get("errotCode").toString();

		Document document = DocumentHelper.createDocument();
		Element response = document.addElement("error");
		Element jobinstanceid = response.addElement("jobinstanceid");
		jobinstanceid.setText(jobinstance);
		Element code = response.addElement("code");
		code.setText(errotCode);
		Element message = response.addElement("message");
		message.setText(errorMessage);

		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(fileName), conf);
			OutputStream out = fs.create(new Path(fileName),
					new Progressable() {
						public void progress() {
						}
					});
			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("UTF-8");
			XMLWriter xmlWriter = new XMLWriter(out, format);
			xmlWriter.write(document);
			xmlWriter.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}
}