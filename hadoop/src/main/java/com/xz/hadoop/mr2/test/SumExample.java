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


@SuppressWarnings("deprecation")
public class SumExample extends Configured implements Tool {
	static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		// 用于缓存 dept文件中的数据
		private Map<String, String> deptMap = new HashMap<String, String>();
		private String[] kv;

		// 此方法会在Map方法执行之前执行且执行一次
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			BufferedReader in = null;
			try {
				Configuration conf = context.getConfiguration();
				System.out.println(conf.get("job_parms"));
				Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				System.out.println(paths.length);
				System.out.println(paths[0]);
				String deptIdName = null;
				for (Path path : paths) {
					// 对部门文件字段进行拆分并缓存到deptMap中
					if (path.toString().contains("dept")) {
						in = new BufferedReader(new FileReader(path.toString()));
						while (null != (deptIdName = in.readLine())) {

							// 对部门文件字段进行拆分并缓存到deptMap中
							// 其中Map中key为部门编号，value为所在部门名称
							deptMap.put(deptIdName.split(",")[0], deptIdName.split(",")[1]);
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
			// 对员工文件字段进行拆分
			System.out.println(deptMap);
			kv = value.toString().split(",");

			// map join: 在map阶段过滤掉不需要的数据，输出key为部门名称和value为员工工资
			if (deptMap.containsKey(kv[7])) {
				if (null != kv[5] && !"".equals(kv[5].toString())) {
					context.write(new Text(deptMap.get(kv[7].trim())), new Text(kv[5].trim()));
				}
			}
		}
	}

	static class ReduceClass extends Reducer<Text, Text, Text, LongWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// 对同一部门的员工工资进行求和
			long sumSalary = 0;
			for (Text val : values) {
				sumSalary += Long.parseLong(val.toString());
			}

			// 输出key为部门名称和value为该部门员工工资总和
			context.write(key, new LongWritable(sumSalary));
		}
	}
	
	    public int run(String[] args) throws Exception {
	 
	    	Configuration conf = new Configuration(); 


	    	conf.setStrings("job_parms", "aaabbc"); //关键就是这一句
	    	
	        // 实例化作业对象，设置作业名称、Mapper和Reduce类
	        Job job = new Job(getConf(), "SumExample");
	        job.setJobName("SumExample");
	       // job.setJarByClass(SumExample.class);
	        job.setMapperClass(MapClass.class);
	        job.setReducerClass(ReduceClass.class);
	 
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

	    public static void main(String[] args) throws Exception {
	        int res = ToolRunner.run(new Configuration(), new SumExample(), args);
	        System.exit(res);
	    }

}
