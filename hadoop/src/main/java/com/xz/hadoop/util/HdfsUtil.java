package com.xz.hadoop.util;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.xz.hadoop.conf.HadoopConf;
import com.xz.hadoop.mr2.util.MRUtil;

public class HdfsUtil {

	private static HdfsUtil operation = new HdfsUtil();
	private Configuration configuration;
	private FileSystem fileSystem ;
	
	private HdfsUtil() {
		configuration = MRUtil.getConfiguration(null) ;
		
		try {
			fileSystem = FileSystem.get(this.configuration) ;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static HdfsUtil getInstance() {
		return operation;
	}

	public boolean removePath(Path path) {
		boolean bo = false;
		int i = this.getFileStatus(path);
		if (i == FileStatusUtil.PATH_NOTEXEIS) {
			bo = true ;
		}else {
			try {
				//子目录强制删除 true 是  false 报错
				fileSystem.delete(path, true);
				bo = true ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return bo;
	}
	
	public boolean put(Path localPath,Path hdfsPath){
		boolean bo = false ;
		try {
			fileSystem.copyFromLocalFile(localPath, hdfsPath);
			bo = true ;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bo ;
	}

	public int getFileStatus(Path path) {
		int i = FileStatusUtil.PATH_NOTEXEIS ;
		try {
			FileStatus fileStatus = null ;
			try {
				fileStatus = fileSystem.getFileStatus(path);
				if (fileStatus.isDirectory()) {
					i = FileStatusUtil.PATH_DIRECTORY ; 
				}
				if (fileStatus.isFile()) {
					i = FileStatusUtil.PATH_FILE ; 
				}
			} catch (FileNotFoundException e) {
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 
		return i ;
	}

	public String showFile(Path path){
		StringBuffer sb = new StringBuffer() ;
		try {
			FileStatus[] fileStatus = fileSystem.listStatus(path) ;
			for (int i = 0; i < fileStatus.length; i++) {
				FileStatus fs = fileStatus[i] ;
				boolean bo = fs.isDirectory() ;
				sb.append(bo?"d":"-").append(fs.getPermission().toString()).append("\t")
						.append(fs.getPath().getName()).append("\n") ;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sb.toString() ;
	}


	
	interface FileStatusUtil{
		 final static int PATH_NOTEXEIS = -1;
		 final static int PATH_FILE = 0;
		 final static int PATH_DIRECTORY = 1;
	}
	
	public boolean mkdir(Path path){
		boolean bo = false ;
		try {
			bo = fileSystem.mkdirs(path) ;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bo ;
	}
	
	public static void main(String[] args) {
		HdfsUtil hdfsUtil = HdfsUtil.getInstance() ;
		String pathStr = HadoopConf.getHdfsPrefixpath()+"/" ;
		Path path = new Path(pathStr) ;
		
	}
}
