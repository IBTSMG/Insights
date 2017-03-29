package com.ibtsmg.insights.dataframes;

import java.util.Map;

public class TransactionDetailDurationsESOut extends TransactionDetailDurationsOut {

	private static final long serialVersionUID = -456457887806452052L;
	
	private Map<String,String> entrypoint;
	
	
	

	public Map<String,String> getEntrypoint() {
		return entrypoint;
	}
	public void setEntrypoint(Map<String,String> entrypoint) {
		this.entrypoint = entrypoint;
	}
	

	
	
}
