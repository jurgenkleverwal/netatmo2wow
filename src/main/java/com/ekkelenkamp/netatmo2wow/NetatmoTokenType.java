package com.ekkelenkamp.netatmo2wow;

public enum NetatmoTokenType {
	ACCESS("access"),
	REFRESH("refresh");
	
	public final String name;

    private NetatmoTokenType(String name) {
        this.name = name;
    }
    
    @Override
    public String toString() {
        return name;
    }
}
