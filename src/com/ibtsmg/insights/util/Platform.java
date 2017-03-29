package com.ibtsmg.insights.util;

public enum Platform {

	CORE(1);
	
	private int platformId;
	
	private Platform(int platformId){
		this.platformId = platformId;
	}
	
	public int getId(){
		return platformId;
	}
	
	public static Platform parse(int id) {
      for (Platform p : Platform.values()) {
        if (id == p.platformId) {
          return p;
        }
      }
	  return null;
	}
	
}
