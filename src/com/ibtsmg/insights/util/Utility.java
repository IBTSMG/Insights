package com.ibtsmg.insights.util;

import java.util.Arrays;
import java.util.Map;


public class Utility {
	
	
	public static String coalesce(Map<String,Object> header, String...keys){
		if(keys == null || keys.length == 0)
			return null;
		if(!isNull(header,keys[0])){
			return (String)header.get(keys[0]);
		}
		
		return coalesce(header,Arrays.copyOfRange(keys,1,keys.length));
	}
	
	public static boolean isNull(Map<String,Object> header, String key){
		return header.get(key) == null || "null".equals(header.get(key));
	}
	
	public static String lpad(String str, char pad, int len){
		if(str == null){
			return null;
		}
		
		if(str.length() >= len){
			return str.substring(0,len);
		}
		
		StringBuilder builder = new StringBuilder();
		int padNum = len - str.length();
		
		for(int i = 0; i<padNum; i++){
			builder.append(pad);
		}
		builder.append(str);
		return builder.toString();
	
		
	}
	
	public static String removeSpecialChars(String in){
		if(in.equals("*") || in.equals(".*")){
			return "all";
		}
		 return in.replaceAll("[^A-Za-z0-9-_]", "-");
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T unsafeCast(Object o) 
	{
	    return (T) o;
	}

	public static ItemType getEntryType(Map<String, Object> header) {
		ItemType trxEntryType = (!isNull(header, "processType") && ((Double)header.get("processType")).intValue() == ProcessType.COREQUEUE.getType() )? ItemType.QUEUE
				: !isNull(header, "screenCode") ? ItemType.SCREEN 
				: !isNull(header, "batchName") ? ItemType.BATCH
				: !isNull(header, "channelTrxName") ? ItemType.CHANNELTRX
				: ItemType.OTHER;
		return trxEntryType;
	}
	

	public static String getEntryName(Map<String, Object> header) {
		
		return (!isNull(header, "processType") && ((Double)header.get("processType")).intValue() == ProcessType.COREQUEUE.getType() )? Utility.coalesce(header,"queueName","null") : Utility.coalesce(header,"screenCode","batchName","channelTrxName","null");
	}


}
