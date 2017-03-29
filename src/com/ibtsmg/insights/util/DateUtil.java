package com.ibtsmg.insights.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
	
	
	private final static ThreadLocal<SimpleDateFormat> dirDateFormatter = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
               return new SimpleDateFormat("yy-MM-dd");
        }
	}; 
	
	private final static ThreadLocal<SimpleDateFormat> dbDateFormatter = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
               return new SimpleDateFormat("yyyyMMdd");
        }
	};
	
	private final static ThreadLocal<SimpleDateFormat> esDateFormatter = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
               return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        }
	};
	private final static ThreadLocal<SimpleDateFormat> dbTimeFormatter = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
               return new SimpleDateFormat("HHmmss");
        }
	};
	
	private final static ThreadLocal<SimpleDateFormat> dbDateTimeFormatter = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
               return new SimpleDateFormat("yyyyMMdd HHmmss");
        }
	};
	
	private final static ThreadLocal<SimpleDateFormat> indexDateTimeFormatter = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
               return new SimpleDateFormat("yyyyMMddHHmmss");
        }
	};
	
	
	public static Date getPreviousMonth(){
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, -2);
		return cal.getTime();
	}
	
	public static Date getYesterday(){
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		return cal.getTime();
	}
	
	public static Date getToday(){
		return Calendar.getInstance().getTime();
	}
	
	public static int getDBDate(Date date){
		String d = dbDateFormatter.get().format(date);
		return Integer.parseInt(d);
		
	}
	
	public static int getDBTime(Date date){
		String d = dbTimeFormatter.get().format(date);
		return Integer.parseInt(d);
		
	}
	
	public static String getDirDate(Date date){
		return dirDateFormatter.get().format(date);
		
	}
	
	public static Date parse(String date) throws ParseException{
		return dbDateFormatter.get().parse(date);
		
	}
	
	public static String getESDate(String date) throws ParseException{
		Date d = dbDateFormatter.get().parse(date);
		return esDateFormatter.get().format(d);
	}
	
	public static String getESDate(String date, String time) throws ParseException{
		String dateTime = date + " " + Utility.lpad(time, '0', 6);
		Date d = dbDateTimeFormatter.get().parse(dateTime);
		return esDateFormatter.get().format(d);
	}
	
	public static String getESDate(long date) throws ParseException{
		Date d = new Date(date);
		return esDateFormatter.get().format(d);
	}
	
	public static String getIndexDate(long millis){
		return indexDateTimeFormatter.get().format(new Date(millis));
		
	}
	

}
