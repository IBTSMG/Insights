package com.ibtsmg.insights.dataframes;

public class TransactionDurationOut extends DurationsOut {

	private static final long serialVersionUID = 252275532915225752L;
	
	private String entryServiceName;
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

	public String getEntryServiceName() {
		return entryServiceName;
	}

	public void setEntryServiceName(String entryServiceName) {
		this.entryServiceName = entryServiceName;
	}
	
	

}
