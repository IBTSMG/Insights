package com.ibtsmg.insights.jobs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.ibtsmg.insights.dataframes.TreeOut;
import com.ibtsmg.insights.util.ESUtility;
import com.ibtsmg.insights.util.ItemType;
import com.ibtsmg.insights.util.JobParams;
import com.ibtsmg.insights.util.LookupUtility;
import com.ibtsmg.insights.util.ServiceInfo;
import com.ibtsmg.insights.util.SparkUtility;
import com.ibtsmg.insights.util.Utility;

import scala.Tuple2;

public class DependencyTreeAnalysis {
	
	public static void main(String[] args) throws Exception {
		
		
		JobParams prm = new JobParams(args);

		JavaSparkContext context = null;
		try{
		    SparkConf conf = new SparkConf().setAppName("DependencyTreeAnalysis");
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
		    
		    
		    
		    
		    JavaRDD<TreeOut> callTrees = context.textFile(inPath.value())
		    												.filter(p -> SparkUtility.corruptDataFilter(p))
						    								.filter(p -> SparkUtility.channelFilter(p,channelCode))
						    								.flatMapToPair(p -> dependencyTreeExtractor(p,pDate))
						    								.reduceByKey((p,q) -> SparkUtility.pathReducer(p, q))
						    								.flatMapToPair(p -> SparkUtility.callTreeFlatter(p))
						    								.map(p -> SparkUtility.treeMapper(p, platform,executionTime));

	   
		    HiveContext hiveContext = SparkUtility.getHiveContext(context);
		    
		    if(sendToHive.value()){
			    DataFrame df = hiveContext.createDataFrame(callTrees,TreeOut.class);
			    df.select("executiontime","platform","channelcode","entrypointname","type","servicename","count","path","channelExecTrxName","channelHostProcessCode","processdate")
			    	.write()
			    	.mode(SaveMode.Append)
			    	.format("orc")
			    	.partitionBy("processdate")
			    	.saveAsTable(hiveSchema.value() + ".dependencytrees");
		    }
		    
		    if(sendToES.value()){
		    	JavaRDD<SparkOut> callTreesEs = callTrees.map(p -> ESUtility.treeESMapper(p)).map(p -> ESUtility.convertToESApplicableFormat(p));
		    	JavaEsSpark.saveToEs(callTreesEs, "dependencytrees_" + processDate.value().substring(0, 4) + (consolidated.value() ? ("_monthly") : "" ) + "/dependencytrees"); 
		    }

		}finally{
			if(context != null)
				context.close();
		}

	}
	
	

	private static Iterable<Tuple2<String,String>> dependencyTreeExtractor(String json, Broadcast<Integer> pDate){
		
		
		Gson gson = new Gson();
		Map<String,Object> map = Utility.unsafeCast(gson.fromJson(json, HashMap.class));
		Map<String,Object> header = Utility.unsafeCast(map.get("header"));
			
		String channelCode = (String)header.get("entranceChannelCode");
		int processDate = Integer.parseInt((String)header.get("processDate"));
		int date = pDate.value() == 0 ? processDate : pDate.value();
		String trxEntryName = Utility.getEntryName(header);
		ItemType trxEntryType = Utility.getEntryType(header);
		
		String channelExecTrxName = Utility.coalesce(header,"channelExecTrxName","null");
		String channelHostProcessCode = Utility.coalesce(header,"channelHostProcessCode","null");
		
		
		ServiceInfo root = new ServiceInfo("root",0,0,0);
		ServiceInfo trxEntry = new ServiceInfo(trxEntryName,1,0,0);
		
		List<Map<String,Object>> services = Utility.unsafeCast(map.get("services"));
		ServiceInfo[] sortedServices = new ServiceInfo[services.size()+2];
		String entryServiceName = null;
		sortedServices[0] = root;
		sortedServices[1] = trxEntry;
		Set<Integer> parents = Collections.newSetFromMap(new HashMap<Integer,Boolean>());
		for(Map<String,Object> service : services){
			String name = ((String)service.get("serviceName"));
			int parent = ((Double)service.get("parent")).intValue();
			int order = ((Double)service.get("order")).intValue();
			ServiceInfo si = new ServiceInfo(name,order,0,parent);
			sortedServices[order] = si;
			parents.add(parent);
			if(order == 2){
				entryServiceName = name;
			}
			
		}
		
		List<Tuple2<String,String>> res = new ArrayList<>();
		for(int i = 2; i < sortedServices.length && sortedServices[i] != null ; i++){
			
			if(parents.contains(i)){
				continue;
			}
			
			ServiceInfo srv = sortedServices[i];
			if(srv.name == null || srv.parent <= 0){
				continue;
			}
			
			StringBuilder pathBuilder = new StringBuilder(srv.name);
			
			ServiceInfo tmp = srv;
			while(tmp != null && tmp.order > 1){
				tmp = sortedServices[tmp.parent];
				pathBuilder.insert(0,"-");
				pathBuilder.insert(0,tmp.name);
			}

			pathBuilder.append(",1");
			res.add(new Tuple2<String,String>(channelCode + "~" + entryServiceName + "~" + date + "~" + trxEntryName + "~" + trxEntryType.getType()+ "~" + channelExecTrxName + "~" + channelHostProcessCode,pathBuilder.toString()));

			
		}
		
		return res;
		
	}



	


}
