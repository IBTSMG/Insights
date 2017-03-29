package com.ibtsmg.insights.util;

public enum ProcessType {

	OLTP(0),
	BATCH(1),
	SFPROCESS(2),
	QUEUEDPROCESS(3),
	QUEUEDTRANSACTIONPROCESS(4),
	CONCURRENT(5),
	COREQUEUEREMOTE(6),
	COREQUEUE(7);
	
	private int type;
	
	private ProcessType(int type){
		this.type = type;
	}
	
	public int getType(){
		return type;
	}
	
	public static ProcessType parse(int type) {
      for (ProcessType p : ProcessType.values()) {
        if (type == p.type) {
          return p;
        }
      }
	  return null;
	}
	
}
