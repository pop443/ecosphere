package com.xz.hbase.mr2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xz.hadoop.mr2.util.MRPaths;
import com.xz.hadoop.mr2.util.MRUtil;
import com.xz.hbase.conf.HbaseConf;

/**
 * 统计一行多少列
 * 
 * @author root
 *
 */
public class HbaseTable2File extends Configured implements Tool {
	/**
	 * ImmutableBytesWritable rowkey Result 获得的行对象,此例 包含的数据跟scan配置项有关
	 * 只定义两个mapper阶段的出参
	 * 
	 * @author root
	 *
	 */
	public static class HbaseTable2FileMapper extends
			TableMapper<ImmutableBytesWritable, IntWritable> {

		@Override
		protected void map(
				ImmutableBytesWritable row,
				Result value,
				Context context)
				throws IOException, InterruptedException {
			ImmutableBytesWritable userkey = new ImmutableBytesWritable(
					row.get());

			context.write(userkey, new IntWritable(1));
		}
	}

	public static class HbaseTable2FileReduce extends
			Reducer<ImmutableBytesWritable, IntWritable, Text, LongWritable> {

		@Override
		protected void reduce(
				ImmutableBytesWritable rowkey,
				Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (IntWritable intWritable : values) {
				sum = sum + 1;
			}
			context.write(new Text(Bytes.toString(rowkey.get())),
					new LongWritable(sum));
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// 实例化作业对象，设置作业名称、Mapper和Reduce类
		Configuration configured = getConf();
		Job job = new Job(configured);

		job.setJobName("HbaseTable2File");
		job.setJarByClass(HbaseTable2File.class);

		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(configured.get("startRow")));
		InclusiveStopFilter filter = new InclusiveStopFilter(
				Bytes.toBytes(configured.get("stopRow")));
		scan.setFilter(filter);
		scan.addFamily(Bytes.toBytes(configured.get("family")));
		scan.setBatch(1);
		scan.setCaching(100);
		// 初始化 hbase mapper 序列化对象
		TableMapReduceUtil.initTableMapperJob(
				TableName.valueOf(configured.get("table")), scan,
				HbaseTable2FileMapper.class,
				ImmutableBytesWritable.class, IntWritable.class, job);
		job.setReducerClass(HbaseTable2FileReduce.class);

		FileOutputFormat.setOutputPath(job, new Path(arg0[0]));

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String outPath = "/result";
		MRPaths args2 = new MRPaths(outPath);
		String[] arg = args2.getArgsPaths();

		Map<String, String> map = new HashMap<String, String>();
		map.put("table", HbaseConf.TABLENAME_STR);
		map.put("family", HbaseConf.FAMILYNAME_STR);
		map.put("startRow", "11000-20030505121209");
		map.put("stopRow", "11000-20030505121210");

		int ret = ToolRunner.run(MRUtil.getConfiguration(map),
				new HbaseTable2File(), arg);
		System.exit(ret);
	}

}