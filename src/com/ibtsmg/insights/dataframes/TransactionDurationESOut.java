package com.ibtsmg.insights.dataframes;

import java.util.Map;

public class TransactionDurationESOut extends DurationsOut{

	private static final long serialVersionUID = 3394166092040214318L;
	
	private String entryServiceName;
	private Map<String,String> entrypoint;

	public Map<String,String> getEntrypoint() {
		return entrypoint;
	}
	public void setEntrypoint(Map<String,String> entrypoint) {
		this.entrypoint = entrypoint;
	}
	public String getEntryServiceName() {
		return entryServiceName;
	}
	public void setEntryServiceName(String entryServiceName) {
		this.entryServiceName = entryServiceName;
	}
	
	

}
