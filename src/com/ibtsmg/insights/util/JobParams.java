package com.ibtsmg.insights.util;

import java.text.ParseException;
import java.util.Date;
import java.util.Properties;

public class JobParams {
	
	private static Properties properties = new Properties();
	private static final String ROOT_DIR = "hdfs://HDP/INSIGHTS";
	
	static{
		properties.put("date", DateUtil.getYesterday());
		properties.put("pDate", "0");
		properties.put("channel", ".*");
		properties.put("platform", "*");
		properties.put("jobname", "NONAME");
		properties.put("sendToHive", "1");
		properties.put("sendToES", "1");
		properties.put("transactionId", "0");
		properties.put("schemaName", "insightsdb");
		properties.put("consolidated", "0");
		properties.put("projectname", "insights");
	}
		
	public JobParams(String ...args) throws ParseException{
		
		for(String arg : args){
			String[] split = arg.split("=");
			String key = split[0];
			String val = split[1];
			
			parseParams(key, val);
			
		}
		
	}

	private void parseParams(String key, String val) throws ParseException {
		switch(key.toLowerCase()){
		case "date":
			if(val.equalsIgnoreCase("yesterday")){
				properties.put("date", DateUtil.getDirDate(DateUtil.getYesterday()));
			}else if(val.equalsIgnoreCase("today")){
				properties.put("date", DateUtil.getDirDate(DateUtil.getToday()));
			}else if(val.equalsIgnoreCase("all")){
				properties.put("date", "*");
			}else if(val.equalsIgnoreCase("prevmonth")){
				Date previousMonth = DateUtil.getPreviousMonth();
				String dirDate = DateUtil.getDirDate(previousMonth);
				int dbDate = DateUtil.getDBDate(previousMonth);
				properties.put("date", dirDate.substring(0,6)+"*");
				properties.put("pDate", (dbDate/100)+"01");
				properties.put("consolidated", "1");
			
			}else if(val.length() == 6){
				properties.put("date", val.substring(2,4) + "-" +  val.substring(4) + "-*");
				properties.put("pDate", val+"01");
				properties.put("consolidated", "1");
			}else{
				properties.put("date", DateUtil.getDirDate(DateUtil.parse(val)));
			}
			break;
		case "channel":
			if(val.equalsIgnoreCase("all")){
				properties.put("channel", ".*");
			}else{
				properties.put("channel", Utility.lpad(val,'0',3));
			}
			break;
		case "platform":
			properties.put("platform", Platform.valueOf(val.toUpperCase()).toString());
			break;
		default: 
			properties.put(key, val);
			break;
		}
	}
	
	public boolean isConsolidated(){
		String cons = Utility.unsafeCast(getProperty("consolidated"));
		return "1".equals(cons);
	}
	
	public String getProcessDate(){
		String date = Utility.unsafeCast(getProperty("date"));
		return date.replaceAll("[^0-9]", "");
	}
	
	public String constructTimeSpecificInputPath(String date, String time){
		return ROOT_DIR + getProperty("projectname") +"/" + getProperty("platform") + "/" + date +"/" + time + "/*";
	}

	
	public String constructInputPath(){
		return ROOT_DIR + getProperty("projectname") +"/" + getProperty("platform") + "/" + getProperty("date") +"/*/*";	
	}
	
	
	public Object getProperty(String key){
		return properties.getProperty(key);
	}

	public String getChannelCode() {
		return Utility.unsafeCast(getProperty("channel"));
	}

	public String getPlatform() {
		return Utility.unsafeCast(getProperty("platform"));
	}
	
	public boolean send2Hive(){
		return "1".equals(Utility.unsafeCast(getProperty("sendToHive")));
	}
	
	public boolean send2ES(){
		return "1".equals(Utility.unsafeCast(getProperty("sendToES")));
	}
	
	public long getTransactionId(){
		return Long.parseLong(Utility.unsafeCast(getProperty("transactionId")));
	}
	
	public String getHiveSchema() {
		return Utility.unsafeCast(getProperty("schemaName"));
	}
	
	public int getPDate(){
		return Integer.parseInt(Utility.unsafeCast(getProperty("pDate")));
	}
	


	

}
