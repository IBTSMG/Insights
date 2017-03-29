package com.ibtsmg.insights.dataframes;

import java.util.Map;

public class TreeESOut extends TreeOut {

	private static final long serialVersionUID = 252275532915225752L;
	
	private Map<String,String> level;	
	private Map<String,String> entrypoint;	

	public Map<String,String> getEntrypoint() {
		return entrypoint;
	}
	public void setEntrypoint(Map<String,String> entrypoint) {
		this.entrypoint = entrypoint;
	}
	public Map<String, String> getLevel() {
		return level;
	}
	public void setLevel(Map<String, String> level) {
		this.level = level;
	}


}
