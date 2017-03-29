package com.ibtsmg.insights.dataframes;


public class TreeOut extends SparkOut {

	private static final long serialVersionUID = 252275532915225752L;
	
	private String entrypointname;
	private String servicename;
	private long count;	
	private String path;
	private String channelExecTrxName;
	private String channelHostProcessCode;
	private int type;

	
	public String getChannelExecTrxName() {
		return channelExecTrxName;
	}
	public void setChannelExecTrxName(String channelExecTrxName) {
		this.channelExecTrxName = channelExecTrxName;
	}
	public String getChannelHostProcessCode() {
		return channelHostProcessCode;
	}
	public void setChannelHostProcessCode(String channelHostProcessCode) {
		this.channelHostProcessCode = channelHostProcessCode;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}

	public String getEntrypointname() {
		return entrypointname;
	}
	public void setEntrypointname(String entrypointname) {
		this.entrypointname = entrypointname;
	}

	public String getServicename() {
		return servicename;
	}
	public void setServicename(String servicename) {
		this.servicename = servicename;
	}		
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	
	
	

}
