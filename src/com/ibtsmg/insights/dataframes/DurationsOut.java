package com.ibtsmg.insights.dataframes;


public class DurationsOut extends SparkOut{

	private static final long serialVersionUID = 6025769458195134856L;
	
	private String name;
	private long totalduration;
	private long callcount;
	private int type;
	
		  
	public int getType() {
		return type;
	}
	public void setType(int itemtype) {
		this.type = itemtype;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public long getTotalduration() {
		return totalduration;
	}
	public void setTotalduration(long totalduration) {
		this.totalduration = totalduration;
	}
	public long getCallcount() {
		return callcount;
	}
	public void setCallcount(long callcount) {
		this.callcount = callcount;
	}

}
