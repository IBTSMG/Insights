package com.ibtsmg.insights.dataframes;

import java.io.Serializable;

import com.ibtsmg.insights.util.ItemType;

public class EntryPoint implements Serializable {

	private static final long serialVersionUID = 2941559935477986761L;
	
	private ItemType type;
	private String screenCode;
	private String batchName;
	private String queueName;
	private String channelTrxName;
	private String channelExecTrxName;
	private String channelHostProcessCode;
	public ItemType getType() {
		return type;
	}
	public void setType(ItemType type) {
		this.type = type;
	}
	public String getScreenCode() {
		return screenCode;
	}
	public void setScreenCode(String screenCode) {
		this.screenCode = screenCode;
	}
	public String getBatchName() {
		return batchName;
	}
	public void setBatchName(String batchName) {
		this.batchName = batchName;
	}
	public String getQueueName() {
		return queueName;
	}
	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}
	public String getChannelTrxName() {
		return channelTrxName;
	}
	public void setChannelTrxName(String channelTrxName) {
		this.channelTrxName = channelTrxName;
	}
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
	
	
	
	
	

}
