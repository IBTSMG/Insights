package com.ibtsmg.insights.dataframes;


public class AccessTimeOut extends SparkOut{

	private static final long serialVersionUID = -8342099490633306603L;
	private int type;
	private String itemname;
	private String accesstime;

	
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	public String getItemname() {
		return itemname;
	}
	public void setItemname(String itemname) {
		this.itemname = itemname;
	}
	public String getAccesstime() {
		return accesstime;
	}
	public void setAccesstime(String accesstime) {
		this.accesstime = accesstime;
	}
	  
	  
	
}
