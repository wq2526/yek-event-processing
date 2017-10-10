package com.kafka.produce.test;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;

public class KafkaClient<K, V> {
	
	private Producer<K, V> producer;
	private Properties props;
	
	public KafkaClient() {
		props = new Properties();
		props.put("bootstrap.servers", "10.109.253.127:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<K, V>(props);
	}
	
	public void produce(K key, V value, String topic) {
		
		producer.send(new ProducerRecord<K, V>(topic, key, value));
		
	}
	
	public void close() {
		producer.close();
	}
	
	public static void main(String[] args) {
		/*KafkaClient<String, String> client = new KafkaClient<String, String>();
		client.produce(null, "{\"event_type\":\"quit\",\"quit\":\"quit\"}", "topic_0");
		client.close();*/
		
		try {
			JSONObject json = new JSONObject("{\"event_type\":\"CDataPoint\",\"ts\":1329900759275,\"index\":\"2556008\",\"mf01\":\"13046\",\"mf02\":\"14389\",\"mf03\":\"8113\",\"pc13\":\"0064\",\"pc14\":\"0191\",\"pc15\":\"0150\",\"pc25\":\"0000\",\"pc26\":\"0000\",\"pc27\":\"0000\",\"bm05\":\"0\",\"bm06\":\"0\",\"bm07\":\"1\",\"bm08\":\"0\",\"bm09\":\"1\",\"bm10\":\"1\"}");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
