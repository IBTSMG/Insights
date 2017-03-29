package com.ibtsmg.insights.dataframes;

public class ImpactCountOut extends SparkOut {
	
	private static final long serialVersionUID = -7467824300775422840L;
	private String module;
	private long count;
	private String itemtype;
	private long totalcount;
	
	public String getModule() {
		return module;
	}
	public void setModule(String module) {
		this.module = module;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	public String getItemtype() {
		return itemtype;
	}
	public void setItemtype(String itemtype) {
		this.itemtype = itemtype;
	}
	public long getTotalcount() {
		return totalcount;
	}
	public void setTotalcount(long totalcount) {
		this.totalcount = totalcount;
	}
	
	

}
