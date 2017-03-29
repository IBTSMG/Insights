package com.ibtsmg.insights.util;

public enum ItemType {

	SCREEN(0),
	BATCH(1),
	CHANNELTRX(2),
	SERVICE(3),
	QUERY(4),
	POM(5),
	CODE(6),
	OTHER(7),
	ATM(8),
	QUEUE(9),
	STOREDPROCEDURE(10),
	CHANNEL(11),
	ESBHOST(12),
	PLATFORM(13),
	MODULE(14),
	REFERENCEDATA(15),
	TABLE(16);
	
	private int type;
	
	private ItemType(int type){
		this.type = type;
	}
	
	public int getType(){
		return type;
	}
	
	public static ItemType parse(int type) {
      for (ItemType p : ItemType.values()) {
        if (type == p.type) {
          return p;
        }
      }
	  return null;
	}
	
}
