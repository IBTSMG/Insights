package com.ibtsmg.insights.util;

import java.util.HashMap;
import java.util.Map;

public class ServiceInfo{
	public String name;
	public int order;
	public long inclusiveDuration;
	public long exclusiveDuration;
	public int parent;
	public String channelId;
	public Platform platform;
	public Map<String,String> components = new HashMap<>();
	
	public ServiceInfo(String name, int order, long duration,int parent){
		this(name,order,duration,parent,"001",Platform.CORE);
	}
	
	public ServiceInfo(String name, int order, long duration,int parent,String channelId,Platform platform) {
		this.name = name;
		this.order = order;
		this.inclusiveDuration = duration;
		this.exclusiveDuration = duration;
		this.parent=parent;
		this.channelId = channelId;
		this.platform = platform;
	}
	
	public void decrementDuration(long val){
		exclusiveDuration = exclusiveDuration - val;
	}
	
	
}
