package com.xz.flume.files.source;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.xz.flume.files.counter.FileCounter;
import com.xz.flume.files.source.plat.BaseSource;
import com.xz.flume.files.source.task.MarkFileTask;
import com.xz.flume.files.source.task.MoveFileTask;
import com.xz.flume.files.source.task.ScanFolderTask;
import com.xz.flume.files.source.utils.FileUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;

import com.xz.flume.files.source.file.FileReaderUtil;
import org.apache.flume.FlumeException;

public class FileSource extends BaseSource {
	/** source名称 */
	private String sourceName;
	/** 扫描文件线程 */
	private ScanFolderTask scanFolderTask;
	/** 持久化文件线程 */
	private MarkFileTask markFileTask ;
	/** 迁移文件线程 */
	private MoveFileTask moveFileTask ;
	/** 统计 */
	private FileCounter fileCounter ;

	@Override
	public void configure(Context context) {
		super.configure(context);

		sourceName = globalContext.get("sourceName");
		//读取文件的主路径
		String read = globalContext.get("read");
		String metaPath = read+"/"+globalContext.get("meta") ;
		String movePath = read+"/"+globalContext.get("move") ;
		//验证文件夹路径
		validate(read);
		//验证主路径下的 各个功能路径
		initDir(metaPath,movePath) ;
		//初始化各个线程
		//文件扫描线程参数
		String fileEnd = globalContext.get("fileEnd") ;
		scanFolderTask = new ScanFolderTask(read, fileCenter,fileEnd) ;
		markFileTask = new MarkFileTask(metaPath,fileCenter) ;
		moveFileTask = new MoveFileTask(movePath,fileCenter) ;

	}
	private void validate(String  parentPath){
		File parentFile = new File(parentPath) ;
		if (!parentFile.exists()) {
			throw new FlumeException("read is not exist!") ;
		}else{
			if (!parentFile.isDirectory()){
				throw new FlumeException("read is not a Directory!") ;
			}
		}
	}
	private void initDir(String... parentPaths) {
		//验证读取文件 的文件夹路径是否正常 存在和是否文件夹
		for (String parentPath:parentPaths) {
			File parentFile = new File(parentPath) ;
			if (parentFile.exists()) {
				if (!parentFile.isDirectory()){
					throw new FlumeException(parentFile.getName()+" is not a Directory!") ;
				}
			}else{
				FileUtil.mkdir(parentFile);
			}
		}
	}

	@Override
	public synchronized void start() {
		super.start();
		fileCounter.start();
		//10s 执行一次持久化
		executor.scheduleWithFixedDelay(markFileTask,0,10, TimeUnit.SECONDS) ;
		//2s 执行一次读取主文件
		executor.scheduleWithFixedDelay(scanFolderTask,0,2, TimeUnit.SECONDS) ;
		//10s 执行一次迁移判断
		executor.scheduleWithFixedDelay(moveFileTask,0,10, TimeUnit.SECONDS) ;
	}

	@Override
	public synchronized void stop() {
		super.stop();
		fileCounter.stop();
	}

	@Override
	public Status process() throws EventDeliveryException {
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		System.out.println("------------process--------------");
		Status status = null;
		List<Event> events = null ;
		try {
			events = FileReaderUtil.readFiles(fileCenter);
			if (events.size()==0){
				Thread.sleep(500);
				return Status.READY ;
			}
			getChannelProcessor().processEventBatch(events);
			status = Status.READY ;
		} catch (IOException e) {
			e.printStackTrace();
			status = Status.BACKOFF ;
		} catch (InterruptedException e){
			status = Status.READY ;
		}
		return status;
	}

}
