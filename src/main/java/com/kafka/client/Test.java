package com.kafka.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class Test {
	
	private KafkaProducerClient<String, String> producer;
	public Test() {
		producer = new KafkaProducerClient<String, String>("10.109.253.145:9092");
		producer.addTopic("input-topic");
	}
	
	public void test() {
		/*for(int i=0;i<1000;i++){
			producer.produce(null, 
					"{\"event_type\":\"person_event\", \"age\":100, \"name\":\"testname3\"}" 
					);
		}
		
		producer.produce(null, 
				"{\"event_type\":\"person_event\", \"age\":200, \"name\":\"testname4\"}" 
				);*/
		producer.produce(null, "{\"event_type\":\"quit\",\"quit\":\"start\"}");
		producer.close();
	}
	
	public void test2() {
		Random random = new Random();
		
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/test/java/data.txt"));
			String line = null;
			while((line=br.readLine())!=null){
				String s = line;
				String[] str = s.split("\t");
				/*try {
					str = s.split("\t");
				} catch (NullPointerException e) {
					// TODO: handle exception
					continue;
				}*/
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS+00:00");
				Date date = simpleDateFormat.parse(str[0]);
				long ts = date.getTime();
				
				StringBuilder eventJson = new StringBuilder();
				eventJson.append("{");
				eventJson.append("\"event_type\":" + "\"DataPoint\",");
				eventJson.append("\"ts\":" + ts + ",");
				eventJson.append("\"index\":" + str[1] + ",");
				eventJson.append("\"bm05\":" + str[2] + ",");
				eventJson.append("\"bm06\":" + str[3] + ",");
				eventJson.append("\"bm07\":" + str[4] + ",");
				eventJson.append("\"bm08\":" + str[5] + ",");
				eventJson.append("\"bm09\":" + str[6] + ",");
				eventJson.append("\"bm10\":" + str[7] + "");
				eventJson.append("}");
				
				producer.produce(null, eventJson.toString());
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			producer.produce(null, "{\"event_type\":\"quit\",\"quit\":\"start\"}");
			producer.close();
		}
	}
	
	public void test3() {
		
		//Random random = new Random();
		
		try {
			for(int i=0;i<3000000;i++){
				
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS+00:00");
				Date date = simpleDateFormat.parse("2012-02-22T16:46:28.9670320+00:00");
				long ts = date.getTime();
				
				StringBuilder eventJson = new StringBuilder();
				eventJson.append("{");
				eventJson.append("\"event_type\":" + "\"DataPoint\",");
				eventJson.append("\"ts\":" + ts + ",");
				eventJson.append("\"index\":" + 0 + ",");
				eventJson.append("\"bm05\":" + 0 + ",");
				eventJson.append("\"bm06\":" + 0 + ",");
				eventJson.append("\"bm07\":" + 0 + ",");
				eventJson.append("\"bm08\":" + 0 + ",");
				eventJson.append("\"bm09\":" + 0 + ",");
				eventJson.append("\"bm10\":" + 0 + "");
				eventJson.append("}");
				
				producer.produce(null, eventJson.toString());
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			producer.produce(null, "{\"event_type\":\"quit\",\"quit\":\"start\"}");
			producer.close();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Test t = new Test();
		t.test3();

	}

}
