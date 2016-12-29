package com.xz.hbase.exception;

public class XZException extends Exception{
	private static final long serialVersionUID = 8032001419536868476L;
	
	public XZException(int code,String message){
		super("XZException："+code+"--"+message);
	}
}
