package com.xz.flume.files.source.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileCenter {

	private static final Logger logger = LoggerFactory.getLogger(FileCenter.class);
	private static FileCenter fileCenter = new FileCenter() ;
	private static Map<String,FileInfo> map ;

	private FileCenter(){

	}
	public static FileCenter newInstance() {
		map = new ConcurrentHashMap<>() ;
		return fileCenter;
	}

	/**
	 * 注册文件
	 *
	 */
	public void registe(List<File> list, Map<String, MarkInfo> markInfoMap) {
			for (File file : list) {
				String absolutePath = file.getAbsolutePath() ;
				if (map.containsKey(absolutePath)) {
					continue ;
				}
				FileInfo fileInfo = null ;
				if (markInfoMap.containsKey(absolutePath)) {
					//有文件的元数据 赋值
					fileInfo = new FileInfo(file.getName(), absolutePath,file.getParent(), file,markInfoMap.get(absolutePath));
				}else{
					fileInfo = new FileInfo(file.getName(), absolutePath,file.getParent(), file,new MarkInfo(absolutePath)) ;
				}
				map.put(absolutePath, fileInfo);
				if (logger.isDebugEnabled()) {
					logger.debug("registe"+absolutePath+"--"+fileInfo.getMarkInfo().toString());
				}
			}
	}

	/**
	 * 返回fileinfo对象与一行日志 的map集合
	 * @return
	 */
	public Map<FileInfo, String> readLine() {
			Map<FileInfo, String> ret = new HashMap<>();
			for (FileInfo fileInfo : map.values()) {
				ret.put(fileInfo, fileInfo.readLine());
			}
			return ret;
	}

	public String getMarkFile(){
		StringBuffer sb = new StringBuffer() ;
		for (FileInfo fileInfo : map.values()) {
			sb.append(fileInfo.getMarkInfo().toString()).append("\r\n") ;
		}
		return sb.toString() ;
	}

	/**
	 * 从全局map中删除fileinfo 并返回删除的fileinfo对象搬迁文件
	 * @param list
	 * @param timeLimit
     */
	public void moveFile(List<FileInfo> list,long timeLimit){
		long now = System.currentTimeMillis() ;
		System.out.println("--------------");
		for (FileInfo fileInfo : map.values()) {
			System.out.println(now+"--"+fileInfo.getModifyTime()+"--"+timeLimit);
			//超时
			if (now-fileInfo.getModifyTime()>timeLimit){

				//设置文件状态为关闭 释放连接
				fileInfo.setState(FileInfo.FileInfoState.CLOSE);
				fileInfo.release();

				map.remove(fileInfo.getAbsolutePath()) ;
				list.add(fileInfo);
			}
		}
		System.out.println("--------------");
	}

}
