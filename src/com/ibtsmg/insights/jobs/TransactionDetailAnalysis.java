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
import com.ibtsmg.insights.dataframes.SparkOut;
import com.ibtsmg.insights.dataframes.TransactionDetailDurationsESOut;
import com.ibtsmg.insights.dataframes.TransactionDetailDurationsOut;
import com.ibtsmg.insights.util.ESUtility;
import com.ibtsmg.insights.util.ItemType;
import com.ibtsmg.insights.util.JobParams;
import com.ibtsmg.insights.util.LookupUtility;
import com.ibtsmg.insights.util.ServiceInfo;
import com.ibtsmg.insights.util.SparkUtility;
import com.ibtsmg.insights.util.Utility;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class TransactionDetailAnalysis {

	private static Tuple2<String, String> serviceDurationCalculator(String s,Broadcast<Integer> pDate){
		
		
		Gson gson = new Gson();
		Map<String,Object> map = Utility.unsafeCast(gson.fromJson(s, HashMap.class));
		
		Map<String,Object> header = Utility.unsafeCast(map.get("header"));

		String trxEntryName = Utility.getEntryName(header);
		
		ItemType trxEntryType = Utility.getEntryType(header);
		
		String channelExecTrxName = null;
		String channelHostProcessCode = null;
		if(trxEntryType == ItemType.CHANNELTRX){
			channelExecTrxName = Utility.coalesce(header,"channelExecTrxName","null");
			channelHostProcessCode = Utility.coalesce(header,"channelHostProcessCode","null");
		}
		
		String entryServiceName = null;
		
		String channelCode = (String)header.get("entranceChannelCode");
		int processDate = Integer.parseInt((String)header.get("processDate"));
		int date = pDate.value() == 0 ? processDate : pDate.value();
		
		List<Map<String,Object>> services = Utility.unsafeCast(map.get("services"));
		    		
		ServiceInfo[] sortedServices = new ServiceInfo[services.size()+2]; 
		for(Map<String,Object> service : services){

			String name = (String)service.get("serviceName");
			int order = ((Double)service.get("order")).intValue();
			long inclusiveDuration = Double.valueOf(((Double)service.get("endTime") - (Double)service.get("startTime"))).longValue();
			int parent = service.get("parent") != null ? ((Double)service.get("parent")).intValue() : 0;
			
			ServiceInfo serviceInfo = new ServiceInfo(name,order,inclusiveDuration,parent);

			sortedServices[order] = serviceInfo;
			
			if(order == 2){
				entryServiceName = name;
			}
									
		}
		
				    		
		for(int i = 2; i<sortedServices.length; i++){
			ServiceInfo si = sortedServices[i];
			if( si != null && si.parent > 1 && sortedServices[si.parent] != null){
				sortedServices[si.parent].decrementDuration(si.inclusiveDuration); 
			}
	
		}
		
		Map<String,String> out = new HashMap<>();
		
		for(int i = 2; i<sortedServices.length; i++){
			ServiceInfo si = sortedServices[i];
			SparkUtility.addOrMergeComponent(out,si.name,si.exclusiveDuration,1L);
		}
		return new Tuple2<String,String>(channelCode + "~" + trxEntryName + "~" + trxEntryType.getType() + "~" + entryServiceName + "~" + date + "~" + channelExecTrxName + "~" + channelHostProcessCode,gson.toJson(out));
	}

	private static String serviceDurationReducer(String a, String b){
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
	
	private static TransactionDetailDurationsOut transactionDetailsOutMapper(Tuple2<String, String> p, Broadcast<String> platform, Broadcast<Long> executionTime) {
		TransactionDetailDurationsOut out = new TransactionDetailDurationsOut();
		String[] keys = p._1.split("~");
		String[] vals = p._2.split("#");
		out.setCallcount(Long.parseLong(vals[2]));
		out.setChannelcode(keys[0]);
		out.setType(Integer.parseInt(keys[2]));
		out.setEntrypointname(keys[1]);
		out.setEntryservicename(keys[3]);
		out.setExecutiontime(executionTime.value());
		out.setName(vals[0]);
		out.setPlatform(platform.value());
		out.setProcessdate(keys[4]);
		out.setTotalduration(Long.parseLong(vals[1]));
		out.setChannelExecTrxName(keys[5]);
		out.setChannelHostProcessCode(keys[6]);
		return out;
	}
	
	private static TransactionDetailDurationsESOut transactionDurationDetailsESMapper(TransactionDetailDurationsOut p) {
		TransactionDetailDurationsESOut out = new TransactionDetailDurationsESOut();
		out.setCallcount(p.getCallcount());
		out.setChannelcode(p.getChannelcode());
		out.setEntryservicename(p.getEntryservicename());
		out.setExecutiontime(p.getExecutiontime());
		out.setName(p.getName());
		out.setPlatform(p.getPlatform());
		out.setProcessdate(p.getProcessdate());
		out.setTotalduration(p.getTotalduration());
		String entryPointName = p.getEntrypointname();
		out.setEntrypointname(entryPointName);		
		ItemType type = ItemType.parse(p.getType());
		Map<String,String> entryPoint = new HashMap<>();
		entryPoint.put("type",type.name());
		switch(type){
		  case SCREEN :
			  entryPoint.put("screenCode",entryPointName);
			  break;
		  case BATCH:
			  entryPoint.put("batchName",entryPointName);
			  break;
		  case QUEUE:
			  entryPoint.put("queueName",entryPointName);
			  break;
		  case CHANNELTRX:
			  entryPoint.put("channelTrxName",entryPointName);
			  entryPoint.put("channelHostProcessCode",p.getChannelHostProcessCode());
			  entryPoint.put("channelExecutingTrxName",p.getChannelExecTrxName());
			  break;
		  default:
			break;
			  
		 }
		  out.setEntrypoint(entryPoint);
		return out;
	}


  public static void main(String[] args) throws Exception {
	  

	  JobParams prm = new JobParams(args);

		JavaSparkContext context = null;
		try{
		    SparkConf conf = new SparkConf().setAppName("TransactionDetailAnalysis");
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
		    
		    
		    
		    JavaRDD<TransactionDetailDurationsOut> trxDurations = context.textFile(inPath.value())
				    												.filter(p -> SparkUtility.corruptDataFilter(p))
								    								.filter(p -> SparkUtility.channelFilter(p,channelCode))
								    								.filter(s -> SparkUtility.transactionSuccessful(s))
								    								.mapToPair(p -> serviceDurationCalculator(p,pDate))
								    								.reduceByKey((p,q) -> serviceDurationReducer(p, q))
								    								.flatMapToPair(p -> flatter(p))
								    								.map(p -> transactionDetailsOutMapper(p, platform,executionTime));

		    HiveContext hiveContext = SparkUtility.getHiveContext(context);
	   
		    if(sendToHive.value()){
			    DataFrame df = hiveContext.createDataFrame(trxDurations,TransactionDetailDurationsOut.class);
			    df.select("executiontime","platform","channelcode","entrypointname","type","entryservicename","name","totalduration","callcount","channelExecTrxName","channelHostProcessCode","processdate")
			    	.write()
			    	.mode(SaveMode.Append)
			    	.format("orc")
			    	.partitionBy("processdate")
			    	.saveAsTable(hiveSchema.value() + ".trxdetails");
		    }

		    
		    
		    if(sendToES.value()){
		    	JavaRDD<SparkOut> serviceDurationsEs = trxDurations.map(p -> transactionDurationDetailsESMapper(p)).map(p -> ESUtility.normalizeDuration(p)).map(p -> ESUtility.convertToESApplicableFormat(p));
		    	JavaEsSpark.saveToEs(serviceDurationsEs, "trxdetails_" + processDate.value().substring(0, 4) + (consolidated.value() ? ("_monthly") : "" ) +  "/trxdetails"); 
		    }

		}finally{
			if(context != null)
				context.close();
		}
		
  }






  
  
 
}
