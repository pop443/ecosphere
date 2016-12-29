package com.xz.hadoop.mr2.statistic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xz.hadoop.mr2.util.MRPaths;
import com.xz.hadoop.mr2.util.MRUtil;

/**
 * MR方式 走内存 select t1.userid,t1.username,t2.deptno,t2.deptname from user t1,dept
 * t2 where t1.deptno=t2.deptno map-side join
 * 1.加入分布式缓存的时候，会把hdfs上的文件拉取到nodemanager机器上，具体路径受yarn-site.xml 参数yarn.nodemanager.local-dirs影响 
 * 2.在读取分布式缓存时，会在nodemanager上生成MR的临时容器包 包括job.jar 步骤1的软连接 之后可以读取软连接下加入 缓存的文件 
 * /mnt/disk01/hadoop/yarn/local/usercache/hadoop/appcache/application_1475130258224_0076/container_1475130258224_0076_01_000001/
 * 	-rw-------	container_tokens
 * 	lrwxrwxrwx	dept -> /mnt/disk01/hadoop/yarn/local/filecache/12/dept
 * 	lrwxrwxrwx	job.jar -> /mnt/disk01/hadoop/yarn/local/usercache/hadoop/appcache/application_1475130258224_0076/filecache/10/job.jar
 * 	drwxr-s---	jobSubmitDir
 * 	lrwxrwxrwx	job.xml -> /mnt/disk01/hadoop/yarn/local/usercache/hadoop/appcache/application_1475130258224_0076/filecache/13/job.xml
 * 	-rwx------	launch_container.sh
 * 	drwxr-s---	tmp
 * 3.通过localfilesystem获得临时容器包下的内存文件夹下的文件 加载入内存
 * 
 * 需要研究map - only 方式  如果不设置
 * 
 * @author root
 *
 */
public class QueryUserDeptName2 extends Configured implements Tool {

	public static class QueryUserDeptNameMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private static Pattern pattern = Pattern.compile(",");
		private static Pattern pattern2 = Pattern.compile(":");
		// <deptNo,depoName>
		private Map<String, String> map = new HashMap<String, String>();

		/**
		 * mapper 阶段从nodemanager机器上取分布式内存文件
		 */
		@Override
		protected void setup(
				Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			String pathStr = "";
			// path 为本地保存分布式内存路径
			// /mnt/disk01/hadoop/yarn/local/usercache/hadoop/appcache/application_1475130258224_0076/container_1475130258224_0076_01_000001/dept
			// 对应于yarn从hdfs上拉取分布式内存的文件夹路径
			Path path = context.getLocalCacheFiles()[0];
			// 获得本地的localfilesystem
			LocalFileSystem localFileSystem = LocalFileSystem
					.newInstanceLocal(context.getConfiguration());
			// 由于path为文件夹，获得此文件夹下的所有文件
			RemoteIterator<LocatedFileStatus> it = localFileSystem.listFiles(
					path, true);
			BufferedReader in = null;
			FileReader fileReader = null;
			String deptLine = null;
			// 循环获取内容
			while (it.hasNext()) {
				LocatedFileStatus fileStatus = it.next();
				// 通过localfilesystem获得的path 前缀为file: 遂截取 通过流方式获取内容加载入内存
				pathStr = fileStatus.getPath().toString().substring(5);
				fileReader = new FileReader(pathStr);
				in = new BufferedReader(fileReader);
				while ((deptLine = in.readLine()) != null) {
					// 首行不处理
					if (deptLine.startsWith("tag:")) {
						continue;
					}
					String deptAll = pattern2.split(deptLine)[1];
					String[] deptInfo = pattern.split(deptAll);
					map.put(deptInfo[0], deptInfo[1]);
				}
			}
			in.close();
			fileReader.close();

		}

		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			// 首行不处理
			if (line.startsWith("tag:")) {
				return;
			}
			// 处理user文件
			String userText = pattern2.split(line)[1];
			String[] userInfo = pattern.split(userText);
			String deptNo = userInfo[3];
			if (map.containsKey(deptNo)) {
				Text outKeyText = new Text(deptNo);
				Text outValueText = new Text(userText + "," + map.get(deptNo));
				context.write(outKeyText, outValueText);
			}
		}

	}


	@Override
	public int run(String[] arg0) throws Exception {
		// 实例化作业对象，设置作业名称、Mapper和Reduce类
		Job job = new Job(getConf());

		job.setJobName("QueryUserDeptName2");

		job.setJarByClass(QueryUserDeptName2.class);
		job.setMapperClass(QueryUserDeptNameMapper.class);

		// 设置输入格式类
		job.setInputFormatClass(TextInputFormat.class);

		// 设置输出格式
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(arg0[0]));
		// user作为驱动表
		FileInputFormat.setInputPaths(job, new Path(arg0[2]));
		job.addCacheFile(new Path(arg0[1]).toUri());

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String outPath = "/result";
		String inPath1 = "/dept";
		String inPath2 = "/user";
		MRPaths args2 = new MRPaths(outPath, inPath1, inPath2);
		String[] arg = args2.getArgsPaths();
		int ret = ToolRunner.run(MRUtil.getConfiguration(null),
				new QueryUserDeptName2(), arg);
		System.exit(ret);
	}

}
