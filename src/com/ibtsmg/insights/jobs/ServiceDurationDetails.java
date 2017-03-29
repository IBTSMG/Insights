package com.ibtsmg.insights.jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.google.gson.Gson;
import com.ibtsmg.insights.dataframes.ServiceDurationDetailsESOut;
import com.ibtsmg.insights.dataframes.ServiceDurationDetailsOut;
import com.ibtsmg.insights.dataframes.SparkOut;
import com.ibtsmg.insights.util.ESUtility;
import com.ibtsmg.insights.util.ItemType;
import com.ibtsmg.insights.util.JobParams;
import com.ibtsmg.insights.util.ServiceInfo;
import com.ibtsmg.insights.util.SparkUtility;
import com.ibtsmg.insights.util.Utility;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ServiceDurationDetails {
	
		
	private static Iterable<Tuple2<String, String>> serviceDurationCalculator(String s, boolean consolidated, int pDate){
		Gson gson = new Gson();
		Map<String,Object> map = Utility.unsafeCast(gson.fromJson(s, HashMap.class));
		
		Map<String,Object> header = Utility.unsafeCast(map.get("header"));
		
		String channelCode = (String)header.get("entranceChannelCode");
		int processDate = Integer.parseInt((String)header.get("processDate"));
		pDate = pDate == 0 ? processDate : pDate;
		int processTime = consolidated ? 0 : (Integer.parseInt((String)header.get("processTime"))/10000)*10000;
		List<Map<String,Object>> services = Utility.unsafeCast( map.get("services"));
		    		
		ServiceInfo[] sortedServices = new ServiceInfo[services.size()+2]; 
				
		for(Map<String,Object> service : services){

			String name = (String)service.get("serviceName");
			int order = ((Double)service.get("order")).intValue();
			long inclusiveDuration = Double.valueOf(((Double)service.get("endTime") - (Double)service.get("startTime"))).longValue();
			int parent = service.get("parent") != null ? ((Double)service.get("parent")).intValue() : 0;
			
			ServiceInfo serviceInfo = new ServiceInfo(name,order,inclusiveDuration,(int)parent);
					    			
			populateInnerDurations(service,"queries","queryName",serviceInfo,ItemType.QUERY);
			populateInnerDurations(service,"poms","pomName",serviceInfo,ItemType.POM);
			
			sortedServices[order] = serviceInfo;
						
		}
				    		
		for(int i = 2; i<sortedServices.length; i++){
			ServiceInfo si = sortedServices[i];
			if( si != null && si.parent > 1 && sortedServices[si.parent] != null){
				SparkUtility.addOrMergeComponent(sortedServices[si.parent].components,si.name+"#"+ ItemType.SERVICE.getType(),si.inclusiveDuration,1L);
				sortedServices[si.parent].decrementDuration(si.inclusiveDuration); 
			}
	
		}
		
		List<Tuple2<String, String>> res = new ArrayList<>();
		for(int i = 2; i<sortedServices.length; i++){
			ServiceInfo si = sortedServices[i];
			SparkUtility.addOrMergeComponent(si.components,"code#"+ ItemType.CODE.getType(),si.exclusiveDuration,1L);
			String json = gson.toJson(si.components);
			res.add(new Tuple2<String,String>(channelCode + "~" + si.name + "~" + pDate + "~" + processTime, json));
		}
		return res;
	}
	
	private static void populateInnerDurations(Map<String,Object> service, String listLabel,String nameLabel, ServiceInfo serviceInfo, ItemType type ) {
		
		List<Map<String,Object>> queries = Utility.unsafeCast(service.get(listLabel));
		
		for(Map<String,Object> query : queries){
			if(!(Boolean)query.get("isExecuted")){
				continue;
			}
			String name = (String)query.get(nameLabel);
			long duration = Double.valueOf(((Double)query.get("endTime") - (Double)query.get("startTime"))).longValue();
			serviceInfo.decrementDuration(duration);
			SparkUtility.addOrMergeComponent(serviceInfo.components,name + "#" + type.getType(), duration, 1L);

		}
		
	}
	
	private static String serviceDurationReducer(String a,String b){
		Gson gson = new Gson();
    	Map<String,String> accumMap = Utility.unsafeCast(gson.fromJson(a, HashMap.class));
    	Map<String,String> newMap = Utility.unsafeCast(gson.fromJson(b, HashMap.class));
		
		for(Entry<String,String> entry : newMap.entrySet()){
			String[] entryVal = ((String)entry.getValue()).split("#");
			SparkUtility.addOrMergeComponent(accumMap, entry.getKey(), Long.parseLong(entryVal[0]), Long.parseLong(entryVal[1]));
		}
		String json = gson.toJson(accumMap);
		return json;
	}
	
	private static ServiceDurationDetailsOut serviceDurationsOutMapper(Tuple2<String, String> p, Broadcast<String> platform, Broadcast<Long> executionTime) {
		ServiceDurationDetailsOut out = new ServiceDurationDetailsOut();
		String[] keys = p._1.split("~");
		out.setChannelcode(keys[0]);
		out.setExecutiontime(executionTime.value());
		String[] component = p._2.split("#");
		out.setCallcount(Long.parseLong(component[3]));
		out.setName(component[0]);
		out.setType(Integer.parseInt(component[1]));
		out.setTotalduration(Long.parseLong(component[2]));
		out.setPlatform(platform.value());
		out.setProcessdate(keys[2]);
		out.setServicename(keys[1]);
		out.setProcesstime(Integer.parseInt(keys[3]));
		return out;
	}
	
	
	private static Iterable<Tuple2<String,String>> flatter(Tuple2<String,String> arg){
		
		List<Tuple2<String, String>> res = new ArrayList<>();
		Gson gson = new Gson();
		Map<String,String> map = Utility.unsafeCast(gson.fromJson(arg._2, HashMap.class));
		for(Entry<String,String> e: map.entrySet()){
			String name = e.getKey();
			String val = e.getValue();
			res.add(new Tuple2<String,String>(arg._1,name + "#" + val));
		}
		return res;
	
		
	}
	
	private static ServiceDurationDetailsESOut serviceDurationDetailsESMapper(ServiceDurationDetailsOut p) {
		ServiceDurationDetailsESOut out = new ServiceDurationDetailsESOut();
		out.setCallcount(p.getCallcount());
		out.setChannelcode(p.getChannelcode());
		out.setExecutiontime(p.getExecutiontime());
		out.setName(p.getName());
		out.setPlatform(p.getPlatform());
		out.setProcessdate(p.getProcessdate());
		out.setServicename(p.getServicename());
		out.setTotalduration(p.getTotalduration());
		out.setItemtype(ItemType.parse(p.getType()).name());
		out.setProcesstime(p.getProcesstime());
		return out;
	}

  public static void main(String[] args) throws Exception {
	  
	JobParams prm = new JobParams(args);

	JavaSparkContext context = null;
	try{
	    SparkConf conf = new SparkConf().setAppName("ServiceDurationDetailAnalysis");
	    context = new JavaSparkContext(conf);
	    final Broadcast<String> channelCode = context.broadcast(prm.getChannelCode());
	    final Broadcast<String> inPath = context.broadcast(prm.constructInputPath());    
	    final Broadcast<String> platform = context.broadcast(prm.getPlatform());  
	    final Broadcast<Boolean> sendToHive = context.broadcast(prm.send2Hive());
	    final Broadcast<Boolean> sendToES = context.broadcast(prm.send2ES());
	    final Broadcast<Long> executionTime = context.broadcast(System.currentTimeMillis());
	    final Broadcast<String> hiveSchema = context.broadcast(prm.getHiveSchema());
	    final Broadcast<Boolean> consolidated = context.broadcast(prm.isConsolidated());
	    final Broadcast<String> processDate =  context.broadcast(prm.getProcessDate());
	    final Broadcast<Integer> pDate =  context.broadcast(prm.getPDate());
	    
	    
	    JavaRDD<ServiceDurationDetailsOut> serviceDurations = context.textFile(inPath.value())
			    												.filter(p -> SparkUtility.corruptDataFilter(p))
							    								.filter(p -> SparkUtility.channelFilter(p,channelCode))
							    								.filter(s -> SparkUtility.transactionSuccessful(s))
							    								.flatMapToPair(p -> serviceDurationCalculator(p,consolidated.value(),pDate.value()))
							    								.reduceByKey((p,q) -> serviceDurationReducer(p, q))
							    								.flatMapToPair(p -> flatter(p))
							    								.map(p -> serviceDurationsOutMapper(p, platform,executionTime));

   
	    if(sendToHive.value()){
		    HiveContext hiveContext = SparkUtility.getHiveContext(context);
		    DataFrame df = hiveContext.createDataFrame(serviceDurations,ServiceDurationDetailsOut.class);
		    df.select("executiontime","platform","channelcode","servicename","name","totalduration","callcount","processtime","processdate")
		    	.write()
		    	.mode(SaveMode.Append)
		    	.format("orc")
		    	.partitionBy("processdate")
		    	.saveAsTable(hiveSchema.value() + ".servicedetails");
	    }
	    
	    if(sendToES.value()){
	    	JavaRDD<SparkOut> serviceDurationsEs = serviceDurations.map(p -> serviceDurationDetailsESMapper(p)).map(p -> ESUtility.normalizeDuration(p)).map(p -> ESUtility.convertToESApplicableFormat(p));
	    	JavaEsSpark.saveToEs(serviceDurationsEs, "servicedetails_" + processDate.value().substring(0, 4) + (consolidated.value() ? ("_monthly") : "_h" ) +  "/servicedetails"); 
	    }

	}finally{
		if(context != null)
			context.close();
	}

  }






  
  
 
}
