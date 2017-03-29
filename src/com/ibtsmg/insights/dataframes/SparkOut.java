package com.ibtsmg.insights.dataframes;

import java.io.Serializable;

public class SparkOut implements Serializable{

	private static final long serialVersionUID = 6025769458195134856L;
	
	private String executiontime;
	private String platform;
	private String channelcode;
	private String processdate;
	private int processtime;

	  
	  
	public String getExecutiontime() {
		return executiontime;
	}
	public void setExecutiontime(long executiontime) {
		this.executiontime = String.valueOf(executiontime);
	}
	public void setExecutiontime(String executiontime) {
		this.executiontime = executiontime;
	}
	public String getPlatform() {
		return platform;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public String getChannelcode() {
		return channelcode;
	}
	public void setChannelcode(String channelcode) {
		this.channelcode = channelcode;
	}
	
	public String getProcessdate() {
		return processdate;
	}
	public void setProcessdate(String processdate) {
		this.processdate = processdate;
	}
	public int getProcesstime() {
		return processtime;
	}
	public void setProcesstime(int processtime) {
		this.processtime = processtime;
	}

	  
	  
	
}
