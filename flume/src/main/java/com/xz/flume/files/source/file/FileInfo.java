package com.xz.flume.files.source.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class FileInfo {
	private static final Logger logger = LoggerFactory.getLogger(FileInfo.class);
	/** 文件名 */
	private String name;
	/** 文件全路径 父路径+文件名 */
	private String absolutePath;
	/** 父路径 */
	private String parentPath ;
	private File file;
	/** 创建时间 */
	private long createTime;
	/** 修改时间 */
	private LineNumberReader lineNumberReader;
	private MarkInfo markInfo;
	private FileInfoState state;

	public FileInfo(String name, String absolutePath,String parentPath, File file, MarkInfo markInfo)  {
		this.name = name;
		this.absolutePath = absolutePath;
		this.parentPath = parentPath;
		this.file = file;
		this.createTime = System.currentTimeMillis() ;
		try {
			lineNumberReader = new LineNumberReader(new InputStreamReader(new FileInputStream(absolutePath), "utf-8"));
		} catch (UnsupportedEncodingException e) {
			state = FileInfoState.EXCEPTION;
			logger.error(e.getMessage());
		} catch (FileNotFoundException e) {
			state = FileInfoState.NOFILE;
			logger.error(e.getMessage());
		}
		this.markInfo = markInfo ;

		initReader();
	}

	private void initReader(){
		try {
			lineNumberReader.skip(this.markInfo.getOffset()) ;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public String readLine() {
		String line = null ;
		if (state == FileInfoState.NORMAL || state == FileInfoState.NORMAL_NODATA
				|| state == FileInfoState.NORMAL_NOMARK) {
			try {
				line = lineNumberReader.readLine() ;
			} catch (IOException e) {
				state = FileInfoState.EXCEPTION ;
			}
		}
		if (line!=null) {
			this.markInfo.autoIncrement(line.length());
			state = FileInfoState.NORMAL ;
		}else{
			state = FileInfoState.NORMAL_NODATA ;
		}
		if (logger.isDebugEnabled()) {
			logger.debug(absolutePath+"--"+markInfo.toString()+"--"+state.toString());
		}

		return line ;
	}

	public void release() {
		try {
			if (lineNumberReader!=null){
				lineNumberReader.close();
				lineNumberReader = null ;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean mark() {
		return markInfo.mark() ;
	}

	public String getName() {
		return name;
	}

	public String getAbsolutePath() {
		return absolutePath;
	}

	public String getParentPath() {
		return parentPath;
	}

	public MarkInfo getMarkInfo() {
		return markInfo;
	}


	public File getFile() {
		return file;
	}

	public void setMarkInfo(MarkInfo markInfo) {
		this.markInfo = markInfo;
	}

	public void setState(FileInfoState state) {
		this.state = state;
	}

	enum FileInfoState {

		/**
		 * 关闭
		 */
		CLOSE,

		/**
		 * 正常
		 */
		NORMAL,

		/**
		 * 读取正常，无新数据
		 */
		NORMAL_NODATA,

		/**
		 * 读取正常，mark失败
		 */
		NORMAL_NOMARK,

		/**
		 * 状态异常
		 */
		EXCEPTION,

		/**
		 * 暂无文件
		 */
		NOFILE;
	}

}
