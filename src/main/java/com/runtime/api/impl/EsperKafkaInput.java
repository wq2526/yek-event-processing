package com.runtime.api.impl;

import com.runtime.api.Input;

public class EsperKafkaInput implements Input {
	
	private String inputTopic;
	
	public EsperKafkaInput(String inputTopic) {
		this.inputTopic = inputTopic;
	}

	public String getInputTopic() {
		return inputTopic;
	}

}
