package com.xz.hadoop.mr2.test;

import java.io.BufferedReader; 
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings("deprecation")
public class Learning {

	static class empMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		// 用于缓存 dept文件中的数据
		private Map<String, String> deptMap = new HashMap<String, String>();
		private String[] kv;

		// 此方法会在Map方法执行之前执行且执行一次
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			BufferedReader in = null;
			try {
				Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				String deptIdName = null;
				for (Path path : paths) {
					System.out.println("path:" + path);
					// 对部门文件字段进行拆分并缓存到deptMap中
					if (path.toString().contains("dept")) {
						in = new BufferedReader(new FileReader(path.toString()));
						while (null != (deptIdName = in.readLine())) {

							// 对部门文件字段进行拆分并缓存到deptMap中
							// 其中Map中key为部门编号，value为所在部门名称
							deptMap.put(deptIdName.split(",")[0], deptIdName.split(",")[1]);
							System.out.println("deptMap:" + deptMap);
						}
					}
				}
			} catch (Exception e) {
				// TODO: handle exception
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
			// TODO Auto-generated method stub
			System.out.print("Before Mapper: " + key + ", " + value);
			String line = value.toString();
			String[] values = line.split(",");
			String dept = values[7];
			String sa = values[5];
			String com = values[6];
			int salary = 0;
			if (sa != null && !sa.equals("")) {
				salary += Integer.parseInt(values[5]);
			}
			if (com != null && !com.equals("")) {
				salary += Integer.parseInt(values[6]);
			}
			context.write(new Text(dept), new IntWritable(salary));
			System.out.println(

					"======" +

			"After Mapper:" + new Text(dept) + ", " + new IntWritable(salary));

		}

		static class empReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
			@Override
			protected void reduce(Text key, Iterable<IntWritable> values, Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				int sum = 0;
				System.out.print("Before Reduce: " + key + ", " + values);
				for (IntWritable value : values) {
					sum += value.get();
				}
				System.out.print("After Reduce: " + key + ", " + values);
				context.write(new Text(key), new IntWritable(sum));
			}

		}

		public static void main(String[] args) throws Exception {

			// 输入路径

			String dst = "hdfs://192.168.99.5:9000/MRlearning/input/emp.txt";

			// 输出路径，必须是不存在的，空文件加也不行。

			String dstOut = "hdfs://192.168.99.5:9000/MRlearning/output/deptsalarysum";

			Configuration hadoopConfig = new Configuration();

			hadoopConfig.set("fs.hdfs.impl",""

					/*DistributedFileSystem.class.getName()*/

			);

			hadoopConfig.set("fs.file.impl",

					org.apache.hadoop.fs.LocalFileSystem.class.getName()

			);

			Job job = new Job(hadoopConfig);

			// 如果需要打成jar运行，需要下面这句

			// job.setJarByClass(NewMaxTemperature.class);

			// job执行作业时输入和输出文件的路径

			FileInputFormat.addInputPath(job, new Path(dst));

			FileOutputFormat.setOutputPath(job, new Path(dstOut));

			// 指定自定义的Mapper和Reducer作为两个阶段的任务处理类

			job.setMapperClass(empMapper.class);

			job.setReducerClass(empReducer.class);

			// 设置最后输出结果的Key和Value的类型

			job.setOutputKeyClass(Text.class);

			job.setOutputValueClass(IntWritable.class);

			// 执行job，直到完成

			job.waitForCompletion(true);

			System.out.println("Finished");

		}

	}
}
