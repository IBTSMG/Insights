package com.ibtsmg.insights.jobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.google.gson.Gson;
import com.ibtsmg.insights.dataframes.CallTreeOut;
import com.ibtsmg.insights.dataframes.SparkOut;
import com.ibtsmg.insights.dataframes.TreeOut;
import com.ibtsmg.insights.util.ESUtility;
import com.ibtsmg.insights.util.ItemType;
import com.ibtsmg.insights.util.JobParams;
import com.ibtsmg.insights.util.ServiceInfo;
import com.ibtsmg.insights.util.SparkUtility;
import com.ibtsmg.insights.util.Utility;

import scala.Tuple2;

public class ServiceCallTreeAnalysis {
	
	public static void main(String[] args) throws Exception {
		
		
		JobParams prm = new JobParams(args);

		JavaSparkContext context = null;
		try{
		    SparkConf conf = new SparkConf().setAppName("ServiceCallTreeAnalysis");
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
		    
		    JavaRDD<CallTreeOut> callTrees = context.textFile(inPath.value())
		    												.filter(p -> SparkUtility.corruptDataFilter(p))
						    								.filter(p -> SparkUtility.channelFilter(p,channelCode))
						    								.flatMapToPair(p -> callTreeExtractor(p,pDate.value()))
						    								.reduceByKey((p,q) -> SparkUtility.pathReducer(p, q))
						    								.flatMapToPair(p -> SparkUtility.callTreeFlatter(p))
						    								.map(p -> SparkUtility.callTreeMapper(p, platform,executionTime));

	   
		    if(sendToHive.value()){
			    HiveContext hiveContext = SparkUtility.getHiveContext(context);
			    DataFrame df = hiveContext.createDataFrame(callTrees,TreeOut.class);
			    
			    df.select("executiontime","platform","channelcode","entrypointname","type","servicename","count","path","channelExecTrxName","channelHostProcessCode","processdate")
			    	.write()
			    	.mode(SaveMode.Append)
			    	.format("orc")
			    	.partitionBy("processdate")
			    	.saveAsTable(hiveSchema.value() + ".calltrees");
		    }
		    
		    if(sendToES.value()){
		    	JavaRDD<SparkOut> callTreesEs = callTrees.map(p -> ESUtility.callTreeESMapper(p)).map(p -> ESUtility.convertToESApplicableFormat(p));
		    	JavaEsSpark.saveToEs(callTreesEs, "calltrees_" + processDate.value().substring(0, 4) + (consolidated.value() ? ("_monthly") : "" ) + "/calltrees"); 
		    }

		}finally{
			if(context != null)
				context.close();
		}

	}
	
	private static Iterable<Tuple2<String,String>> callTreeExtractor(String json, int pDate){
		
		Gson gson = new Gson();
		Map<String,Object> map = Utility.unsafeCast(gson.fromJson(json, HashMap.class));
		Map<String,Object> header = Utility.unsafeCast(map.get("header"));
			
		String channelCode = (String)header.get("entranceChannelCode");
		int processDate = Integer.parseInt((String)header.get("processDate"));
		pDate = pDate == 0 ? processDate : pDate;
		String trxEntryName = Utility.getEntryName(header);
		ItemType trxEntryType = Utility.getEntryType(header);
		
		
		String channelExecTrxName = Utility.coalesce(header,"channelExecTrxName","null");
		String channelHostProcessCode = Utility.coalesce(header,"channelHostProcessCode","null");
		
		ServiceInfo root = new ServiceInfo("root",0,0,0);
		ServiceInfo trxEntry = new ServiceInfo(trxEntryName,1,0,0);
		
		List<Map<String,Object>> services = Utility.unsafeCast(map.get("services"));
		ServiceInfo[] sortedServices = new ServiceInfo[services.size()+2];
		
		sortedServices[0] = root;
		sortedServices[1] = trxEntry;
		
		
		for(Map<String,Object> service : services){
			String name = ((String)service.get("serviceName"));
			int parent = ((Double)service.get("parent")).intValue();
			int order = ((Double)service.get("order")).intValue();
			ServiceInfo si = new ServiceInfo(name,order,0,parent);
			sortedServices[order] = si;
		}
		
		List<Tuple2<String,String>> res = new ArrayList<>();
		for(int i = 2; i < sortedServices.length && sortedServices[i] != null ; i++){
			
			ServiceInfo srv = sortedServices[i];
			if(srv.name == null || srv.parent <= 0){
				continue;
			}
			StringBuilder pathBuilder = new StringBuilder(srv.name);
			ServiceInfo tmp = srv;
			while(tmp != null && tmp.order > 1){
				tmp = sortedServices[tmp.parent];
				pathBuilder.append("-");
				pathBuilder.append(tmp.name);
			}
			if(channelExecTrxName != null){
				pathBuilder.append("-");
				pathBuilder.append(channelExecTrxName);
			}
			pathBuilder.append(",1");
			res.add(new Tuple2<String,String>(channelCode + "~" + srv.name + "~" + pDate + "~" + trxEntryName + "~" + trxEntryType.getType()+ "~" + channelExecTrxName + "~" + channelHostProcessCode + "~" + ItemType.SERVICE.getType(),pathBuilder.toString()));

			
		}
		
		return res;
		
	}
	


}
