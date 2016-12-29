package com.xz.hbase.exception;

public class ExceptionUtils {
	
	public enum Error{
		
		ConnectionIsNull(-1,"链接为null");
		
		private int code ;
		private String message ;
		private Error(int code,String message){
			this.code = code ;
			this.message = message ;
		}
		public XZException wrong(){
			return new XZException(this.code,this.message) ;
		}
		
	}
	public static void main(String[] args) {
		System.out.println(Error.ConnectionIsNull.wrong());
	}
	
}
