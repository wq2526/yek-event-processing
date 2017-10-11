package com.runtime.api.impl;

import java.util.ArrayList;
import java.util.List;

public class EventType {
	
	private String eventType;
	private List<String> eventProps;
	private List<String> propClasses;
	
	public EventType(String eventName) {
		this.eventType = eventName;
		this.eventProps = new ArrayList<String>();
		this.propClasses = new ArrayList<String>();
	}
	
	public void addProp(String prop) {
		eventProps.add(prop);
	}
	
	public void addClass(String className) {
		propClasses.add(className);
	}

}
