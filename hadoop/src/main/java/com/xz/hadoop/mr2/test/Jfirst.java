package com.xz.hadoop.mr2.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/*
 * 题目:J字母开头的员工及其部门名称
 */
@SuppressWarnings("deprecation")
public class Jfirst extends Configured implements Tool {
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Map<String, String> map = new HashMap<>();
		private String[] kv;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			BufferedReader in = null;
			try {
				// 从当前作业中获取要缓存的文件
				Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				String deptIdName = null;
				for (Path path : paths) {
					// 对部门文件字段进行拆分并缓存到deptMap中
					if (path.toString().contains("dept")) {
						// 这种写法是读取缓存文件，在hdfs上读取后再存入本机(节点机)，如果在本机调试，就采用下列写法
						// 如果需要在linux或集群下运行，直接采取下列注释写法
						String path1 = path.toString().substring(5);
						String path2 = "D://" + path1;
						in = new BufferedReader(new FileReader(path2));
						// in = new BufferedReader(new
						// FileReader(path.toString()));
						while (null != (deptIdName = in.readLine())) {
							// 对部门文件字段进行拆分并缓存到deptMap中
							// 其中Map中key为部门编号，value为所在部门名称
							map.put(deptIdName.split(",")[0], deptIdName.split(",")[1]);
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (in != null) {
						in.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line=new String(value.getBytes(),0,value.getLength(),"GBK");
			kv = line.toString().split(",");
			String dept = map.get(kv[7]);
			if (map.containsKey(kv[7])) {
				context.write(new Text(dept), new Text(kv[1].trim()));
			}
		}
	}

	static class MyReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				String name = value.toString();
				if(name.startsWith("J")){
					context.write(key, new Text(name));
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		// 实例化作业对象，设置作业名称、Mapper和Reduce类
		Job job = new Job(getConf(), "Q3EarliestEmp");
		job.setJobName("Q3EarliestEmp");
		//job.setJarByClass(EarliestEmp.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);

		// 设置输入格式类
		job.setInputFormatClass(TextInputFormat.class);

		// 设置输出格式
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	/**
	 * 主方法，执行入口
	 * 
	 * @param args
	 *            输入参数
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Jfirst(), args);
		System.exit(res);
	}
}
