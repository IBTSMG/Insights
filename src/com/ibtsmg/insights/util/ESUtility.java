package com.ibtsmg.insights.util;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import com.ibtsmg.insights.dataframes.CallTreeESOut;
import com.ibtsmg.insights.dataframes.CallTreeOut;
import com.ibtsmg.insights.dataframes.DurationsOut;
import com.ibtsmg.insights.dataframes.SparkOut;
import com.ibtsmg.insights.dataframes.TreeESOut;
import com.ibtsmg.insights.dataframes.TreeOut;

public class ESUtility {

	public static SparkOut convertToESApplicableFormat(SparkOut p) throws ParseException {
		p.setProcessdate(DateUtil.getESDate(p.getProcessdate(),String.valueOf(p.getProcesstime())));
		p.setExecutiontime(DateUtil.getESDate(Long.parseLong(p.getExecutiontime())));
		return p;
	}

	public static CallTreeESOut callTreeESMapper(CallTreeOut p) {
			CallTreeESOut out = new CallTreeESOut();
			out.setChannelcode(p.getChannelcode());
			out.setCount(p.getCount());
			out.setExecutiontime(p.getExecutiontime());
			String entryPointName = p.getEntrypointname();
			out.setEntrypointname(entryPointName);		
			out.setItemname(p.getServicename());
			out.setItemtype(ItemType.parse(p.getItemtype()).name());
			
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
			
			
			out.setPlatform(p.getPlatform());
			out.setProcessdate(p.getProcessdate());
			String[] services = p.getPath().split("-");
			Map<String,String> map = new HashMap<>();
			int i = 1;
			for(String s:services){
				map.put(""+i,s);
				i++;
			}
			out.setLevel(map);
			return out;
		
	}

	public static TreeESOut treeESMapper(TreeOut p) {
		TreeESOut out = new TreeESOut();
		out.setChannelcode(p.getChannelcode());
		out.setCount(p.getCount());
		out.setExecutiontime(p.getExecutiontime());
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
		
		
		out.setPlatform(p.getPlatform());
		out.setProcessdate(p.getProcessdate());
		out.setServicename(p.getServicename());
		String[] services = p.getPath().split("-");
		Map<String,String> map = new HashMap<>();
		int i = 1;
		for(String s:services){
			map.put(""+i,s);
			i++;
		}
		out.setLevel(map);
		return out;
	}

	public static DurationsOut normalizeDuration(DurationsOut p){
		if(p.getTotalduration() == 0L)
			p.setTotalduration(1L);
		return p;
	}

}
