package com.ibtsmg.insights.dataframes;



public class CountOut extends SparkOut{

	private static final long serialVersionUID = 6025769458195134856L;
	
	  private String entryservicename;
	  private long innercallcount;
	  private long callcount;
	  
	
	public String getEntryservicename() {
		return entryservicename;
	}
	public void setEntryservicename(String entryservicename) {
		this.entryservicename = entryservicename;
	}
	public long getInnerCallCount() {
		return innercallcount;
	}
	public void setInnerCallCount(long innercallcount) {
		this.innercallcount = innercallcount;
	}
	public long getCallcount() {
		return callcount;
	}
	public void setCallcount(long callcount) {
		this.callcount = callcount;
	}

}
