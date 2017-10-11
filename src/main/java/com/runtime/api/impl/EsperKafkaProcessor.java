package com.runtime.api.impl;

import java.util.ArrayList;
import java.util.List;

import com.runtime.api.Processor;

public class EsperKafkaProcessor implements Processor {
	
	private List<EventType> eventTypes;
	private List<String> epls;
	private String outType;
	
	public EsperKafkaProcessor() {
		this.eventTypes = new ArrayList<EventType>();
		this.epls = new ArrayList<String>();
		outType = "";
	}
	
	public void addEventType(EventType eventType) {
		eventTypes.add(eventType);
	}
	
	public void addEpl(String epl) {
		epls.add(epl);
	}
	
	public void setOutType(String outType) {
		this.outType = outType;
	}

}
