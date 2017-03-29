package com.ibtsmg.insights.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class LookupUtility {

	public static Broadcast<Map<String, String>> getLookup(JavaSparkContext context,HiveContext hiveContext,String sql) {
		Row[] rows = hiveContext.sql(sql).collect();
		Map<String,String> map = new HashMap<>();
		for(Row r:rows){
			map.put(r.getString(r.fieldIndex("key")),r.getString(r.fieldIndex("value")));
			
		}
		Broadcast<Map<String,String>> lookup = context.broadcast(map);
		return lookup;
	}

	public static String getSPLookupQuery(final Broadcast<String> hiveSchema) {
		return getLookupQuery(hiveSchema,"sp");
	}
	
	public static String getESBHostLookupQuery(final Broadcast<String> hiveSchema) {
		return getLookupQuery(hiveSchema,"hosts");
	}
	
	public static String getModuleLookupQuery(final Broadcast<String> hiveSchema) {
		return getLookupQuery(hiveSchema,"modules");
	}
	
	public static String getChannelLookupQuery(final Broadcast<String> hiveSchema) {
		return getLookupQuery(hiveSchema,"channels");
	}
	
	public static String getLookupQuery(final Broadcast<String> hiveSchema, String tablename) {
		return "select  channelcode as key,description as value from "+hiveSchema.value() + "." + tablename;
	}

}
