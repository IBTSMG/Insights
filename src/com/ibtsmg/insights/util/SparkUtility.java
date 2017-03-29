package com.ibtsmg.insights.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.hive.HiveContext;

import com.google.gson.Gson;
import com.ibtsmg.insights.dataframes.CallTreeOut;
import com.ibtsmg.insights.dataframes.DurationsOut;
import com.ibtsmg.insights.dataframes.TreeOut;

import scala.Tuple2;
import scala.Tuple3;

public class SparkUtility {
	
	public static boolean transactionIdFilter(String s, final Broadcast<Long> transactionId){
		if(transactionId.value().longValue() == 0L){
			return true;
		}
		Gson gson = new Gson();
		Map<String,Object> map = Utility.unsafeCast(gson.fromJson(s, HashMap.class));
		Map<String,Object> header = Utility.unsafeCast(map.get("header"));
		long trxId =((Double)header.get("transactionID")).longValue();
		return trxId == transactionId.value().longValue();
		
	}
	
	public static boolean channelFilter(String s, final Broadcast<String> channelCode){
		if(channelCode.value().equals("*") || channelCode.value().equals(".*") )
			return true;
		Gson gson = new Gson();
		Map<String,Object> map = Utility.unsafeCast(gson.fromJson(s, HashMap.class));
		Map<String,Object> header = Utility.unsafeCast(map.get("header"));
		String channel =(String)header.get("entranceChannelCode");
		return channel.matches(channelCode.value()); 
	}
	
	public static boolean inWorkingHours(String s){
		Gson gson = new Gson();
		Map<String,Object> map = Utility.unsafeCast(gson.fromJson(s, HashMap.class));
		Map<String,Object> header = Utility.unsafeCast(map.get("header"));
		int time = Integer.parseInt((String)header.get("processTime"));
		return time >90000 && time<180000; 
	}
	
	public static boolean between930and1010(String s){
		Gson gson = new Gson();
		Map<String,Object> map = Utility.unsafeCast(gson.fromJson(s, HashMap.class));
		Map<String,Object> header = Utility.unsafeCast(map.get("header"));
		int time = Integer.parseInt((String)header.get("processTime"));
		return time >93000 && time<101000; 
	}
	
	public static boolean transactionSuccessful(String s){
		Gson gson = new Gson();
		Map<String,Object> map = Utility.unsafeCast(gson.fromJson(s, HashMap.class));
		Map<String,Object> header = Utility.unsafeCast(map.get("header"));
		int state = ((Double)header.get("processState")).intValue();
		return state == 3; 
	}
	
	public static void addOrMergeComponent(Map<String,String> components,String name, long duration, long count){
		String tuple2 = components.get(name);
		if(tuple2 == null){
			components.put(name, duration+"#"+count);
			return;
		}
		String[] split = tuple2.split("#");
		components.put(name, (duration + Long.parseLong(split[0])) + "#" + (count + Long.parseLong(split[1])));
	
	}
	

	
	public static DurationsOut durationsOutMapper(Tuple2<String, Tuple2<Long, Long>> arg0,final Broadcast<String> platform, Broadcast<Long> executionTime){
		DurationsOut out = new DurationsOut();
		String[] keys = arg0._1.split("~");
		out.setCallcount(arg0._2._2);
		out.setChannelcode(keys[0]);
		out.setExecutiontime(executionTime.value());
		out.setPlatform(platform.value());
		out.setProcessdate(keys[2]);
		out.setName(keys[1]);
		out.setTotalduration(arg0._2._1);
		return out;
	}
	
	public static Tuple2<Long,Long> longTupleReducer(Tuple2<Long,Long> a, Tuple2<Long,Long> b){
		return new Tuple2<Long,Long>(a._1 + b._1 , a._2 + b._2);
	}
	
	public static Tuple3<Long,Long,Long> longTupleReducer(Tuple3<Long,Long,Long> a, Tuple3<Long,Long,Long> b){
		return new Tuple3<Long,Long,Long>(a._1()+ b._1() , a._2() + b._2(), a._3() + b._3());
	}
	
	public static HiveContext getHiveContext(JavaSparkContext context) {
		HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(context.sc());
		hiveContext.setConf("hive.exec.dynamic.partition", "true");
		hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
		
		return hiveContext;
	}
	
	public static void mergeDuration(Map<String, Tuple2<Long, Long>> durations, String listLabel, long duration) {
		Tuple2<Long, Long> d = durations.get(listLabel);
		if(d == null){
			durations.put(listLabel, new Tuple2<Long,Long>(duration, 1L));
		}
		else{
			durations.put(listLabel, new Tuple2<Long,Long>(d._1 + duration,d._2 + 1L));
		}
		
	}
	
	public static boolean corruptDataFilter(String s){
		
		try{
			Gson gson = new Gson();
	  		Map<String,Object> map = Utility.unsafeCast(gson.fromJson(s, HashMap.class));
	  		
	  		List<Map<String,Object>> services = Utility.unsafeCast( map.get("services"));
	
	  		if(services == null) return false;
	  		
	  		int[] sortedServices = new int[services.size()]; 
	  		int i = 0;
			
			for(Map<String,Object> service : services){
				if(service.get("serviceName") == null ){
					return false;
				}
				int order = ((Double)service.get("order")).intValue();
				sortedServices[i] = order-2;
				i++;			
			}
			
			Arrays.sort(sortedServices);
			for(int j = 0; j<sortedServices.length; j++){
				if(sortedServices[j] != j) return false;
			}
			
			return true;
		}catch(Exception e){
			return false;
		}

	}

	public static String pathReducer(String a, String b){
		String[] services = b.split(";");
		List<String> accumulatedServices = Arrays.asList(a.split(";"));
		List<String> additionalServices = new ArrayList<>();
		
		for(String srv : services){
			String service = srv.split(",")[0];
			int iAccum = indexOf(accumulatedServices,service);
			int iAdd = indexOf(additionalServices,service) ;
			if(iAccum < 0 && iAdd < 0){
				additionalServices.add(srv);
			}else if(iAccum >= 0){
				incrementCount(accumulatedServices, iAccum);
				
			}else{
				incrementCount(additionalServices, iAdd);
				
			}
		}
		
		if(additionalServices.isEmpty())
			return format(accumulatedServices);
		
		return format(accumulatedServices) +";" + format(additionalServices);
	}

	
	private static String format(List<String> services) {
		StringBuilder servicesBuilder = new StringBuilder();
    	int index = 0;
    	for(String s:services){
    		if(index>0){
    			servicesBuilder.append(";");
    		}
    		servicesBuilder.append(s);
    		index++;
    	}
    	return servicesBuilder.toString();
	}
     
	private static void incrementCount(List<String> additionalServices, int iAdd) {
		String[] temp = additionalServices.get(iAdd).split(",");
		long count = Long.parseLong(temp[1]) + 1;
		String newVal = temp[0]+","+count;
		additionalServices.set(iAdd, newVal);
	}

	private static int indexOf(List<String> services, String newService) {
		for(int i = 0 ; i < services.size() ; i++){
			String service = services.get(i).split(",")[0];
			if(service.equals(newService)){
				return i;
			}
			
		}
		return -1;
	}

	public static Iterable<Tuple2<String,String>> callTreeFlatter(Tuple2<String,String> arg){
		
		List<Tuple2<String,String>> res = new ArrayList<>();
		String[] values = arg._2.split(";");
		for(String val: values){
			res.add(new Tuple2<String,String>(arg._1,val));
		}
		return res;
		
	}

	public static TreeOut treeMapper(Tuple2<String,String> arg,final Broadcast<String> platform, Broadcast<Long> executionTime){
		TreeOut callTree = new TreeOut();
		  String[] keyVals = arg._1.split("~");
		  String entryPointName = keyVals[0];
		  callTree.setChannelcode(entryPointName);
		  callTree.setEntrypointname(keyVals[3]);
		  callTree.setExecutiontime(executionTime.value());
		  callTree.setPlatform(platform.value());
		  callTree.setProcessdate(keyVals[2]);
		  callTree.setServicename(keyVals[1]);
		  callTree.setChannelExecTrxName(keyVals[5]);
		  callTree.setChannelHostProcessCode(keyVals[6]);
		  callTree.setType(Integer.parseInt(keyVals[4]));

		  String[] tmp = arg._2.split(",");
		  callTree.setCount(Integer.parseInt(tmp[1]));
		  callTree.setPath(tmp[0]);

		  return callTree;
		  
		  
		  
	  }
	
	
	public static CallTreeOut callTreeMapper(Tuple2<String, String> p, Broadcast<String> platform,Broadcast<Long> executionTime) {
		CallTreeOut callTree = new CallTreeOut();
		  String[] keyVals = p._1.split("~");
		  String entryPointName = keyVals[0];
		  callTree.setChannelcode(entryPointName);
		  callTree.setEntrypointname(keyVals[3]);
		  callTree.setExecutiontime(executionTime.value());
		  callTree.setPlatform(platform.value());
		  callTree.setProcessdate(keyVals[2]);
		  callTree.setServicename(keyVals[1]);
		  callTree.setChannelExecTrxName(keyVals[5]);
		  callTree.setChannelHostProcessCode(keyVals[6]);
		  callTree.setType(Integer.parseInt(keyVals[4]));
	
		  String[] tmp = p._2.split(",");
		  callTree.setCount(Integer.parseInt(tmp[1]));
		  callTree.setPath(tmp[0]);
		  callTree.setItemtype(Integer.parseInt(keyVals[7]));
	
		  return callTree;
 
	}
	

}
