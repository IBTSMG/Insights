package com.ibtsmg.insights.dataframes;

public class TransactionDetailDurationsOut extends DurationsOut {

	private static final long serialVersionUID = -456457887806452052L;
	
	private String entryservicename;
	private String entrypointname;
	private String channelExecTrxName;
	private String channelHostProcessCode;
	
	
	
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
	public String getEntryservicename() {
		return entryservicename;
	}
	public void setEntryservicename(String entryservicename) {
		this.entryservicename = entryservicename;
	}
	public String getEntrypointname() {
		return entrypointname;
	}
	public void setEntrypointname(String entrypointname) {
		this.entrypointname = entrypointname;
	}
	
	
}
