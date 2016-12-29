package com.xz.hbase.mr2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xz.hadoop.mr2.util.MRPaths;
import com.xz.hadoop.mr2.util.MRUtil;
import com.xz.hbase.conf.HbaseConf;

public class File2HbaseTable extends Configured implements Tool {

	public static class File2HbaseTableMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private static Pattern pattern = Pattern.compile(",");
		private static Pattern pattern2 = Pattern.compile(":");

		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] userInfo = pattern.split(pattern2.split(line)[1]);
			String qualifier = userInfo[1];
			String v = userInfo[3];
			context.write(new Text(qualifier), new Text(v));
		}
	}

	/**
	 * mapper 来的两个参数
	 * ImmutableBytesWritable 对应表明
	 * Mutation  put get scan delete 的父类对象
	 * @author root
	 *
	 */
	public static class File2HbaseTableReduce extends
			TableReducer<Text, Text, ImmutableBytesWritable> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();
			ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(
					Bytes.toBytes(configuration.get("table")));
			Put put = new Put(Bytes.toBytes(configuration.get("rowkey")));
			for (Text text : values) {
				put.addColumn(Bytes.toBytes(configuration.get("family")),
						Bytes.toBytes(key.toString()),
						Bytes.toBytes(text.toString()));
			}
			context.write(immutableBytesWritable, put);
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration configured = getConf();
		Job job = new Job(configured);

		job.setJobName("File2HbaseTable");

		job.setJarByClass(File2HbaseTable.class);
		job.setMapperClass(File2HbaseTableMapper.class);

		TableMapReduceUtil.initTableReducerJob(configured.get("table"),
				File2HbaseTableReduce.class, job);
		// 设置输入格式类
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(arg0[1]));

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String inPath = "/user";
		MRPaths args2 = new MRPaths(null, inPath);
		String[] arg = args2.getArgsPaths();

		Map<String, String> map = new HashMap<String, String>();
		map.put("table", HbaseConf.TABLENAME_STR);
		map.put("family", HbaseConf.FAMILYNAME_STR);
		map.put("rowkey", "11000-20030505121208");

		int ret = ToolRunner.run(MRUtil.getConfiguration(map),
				new File2HbaseTable(), arg);
		System.exit(ret);
	}

}
