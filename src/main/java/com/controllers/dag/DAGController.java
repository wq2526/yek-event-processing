package com.controllers.dag;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.client.KafkaProducerClient;
import com.yarn.esper.client.EsperYarnClient;

@RestController
@EnableAutoConfiguration
public class DAGController {
	
	private static final Log LOG = LogFactory.getLog(DAGController.class);
	private ExecutorService exec;
	
	@RequestMapping("/dag")
	public void startYarnClient(@RequestParam(value="data") String json) {
		
		exec = Executors.newCachedThreadPool();
		//exec.execute(new KafkaRunnable());
			
		LOG.info("send json to yarn client: " + json);	
			
		exec.execute(new YarnClientRunnable(json));
		
	}
	
	private class YarnClientRunnable implements Runnable {
		
		private String json;
		private EsperYarnClient client;
		
		public YarnClientRunnable(String json) {
			this.json = json;
			
			client = new EsperYarnClient();
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			client.runYarnClient(json);
		}
		
	}
	
	private class KafkaRunnable implements Runnable {
		
		private KafkaProducerClient<String, String> producer;
		
		public KafkaRunnable() {
			producer = new KafkaProducerClient<String, String>("10.109.253.145:9092");
			producer.addTopic("node1-topic");
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			//test2();
			
			test();
			
		}
		
		private void test() {
			producer.produce(null, 
					"{\"event_type\":\"person_event\", \"age\":100, \"name\":\"testname3\"}" 
					);
			producer.produce(null, 
					"{\"event_type\":\"person_event\", \"age\":200, \"name\":\"testname4\"}" 
					);
			producer.produce(null, "{\"event_type\":\"quit\",\"quit\":\"nodestart\"}");
			producer.close();
		}
		
		private void test2() {
			Random random = new Random();
			
			try {
				BufferedReader br = new BufferedReader(new FileReader("src/test/java/allData.txt"));
				String line = null;
				for(int i=0;i<1000000;i++){
				//while((line=br.readLine())!=null){
					String s = line;
					String[] str = s.split("\t");
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
				producer.produce(null, "{\"event_type\":\"quit\",\"quit\":\"nodestart\"}");
				producer.close();
			}
		}
		
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SpringApplication.run(DAGController.class, args);

	}

}
