package com.app.analytics.cf.utils;

public class ResultSetUtilOutputFormatter{
	public String format(Object value, String columnName) {
		return format(value);
	}
	
	public String format(Object value){
		return String.valueOf(value).trim();
	}
}
