package com.controllers.dag;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.csvreader.CsvReader;
import com.dag.api.DAG;
import com.dag.api.Edge;
import com.dag.api.Vertex;
import com.kafka.produce.test.JsonFileReader;
import com.kafka.produce.test.KafkaClient;
import com.runtime.api.Input;
import com.runtime.api.Processor;
import com.runtime.api.impl.EsperKafkaInput;
import com.runtime.api.impl.EsperKafkaOutput;
import com.runtime.api.impl.EsperKafkaProcessor;
import com.runtime.api.impl.EventType;
import com.yarn.esper.client.EsperYarnClient;

@RestController
@EnableAutoConfiguration
public class DAGController {
	
	private static final Log LOG = LogFactory.getLog(DAGController.class);
	private ExecutorService exec;
	
	@RequestMapping("/dag")
	public void startYarnClient(@RequestParam(value="data") String json) throws JSONException {
		
		exec = Executors.newCachedThreadPool();
		//exec.execute(new KafkaRunnable());
		
		JSONObject jsonObject = new JSONObject(json);
		JSONArray jsonArray = jsonObject.getJSONArray("nodes");
		
		DAG dag = convertToDag(json);
		for(int i=0;i<jsonArray.length();i++){
			JSONObject nodeJson = jsonArray.getJSONObject(i);
			int nodeId = nodeJson.getInt("id");
			Vertex vertex = dag.getVertex(nodeId);
			String input = vertex.getDataSource().getInputTopic();
			String output = vertex.getDataSink().getOutputTopic();
			
			StringBuilder parents = new StringBuilder();
			List<Edge> edges = vertex.getInputEdges();
			for(Edge edge : edges){
				parents.append(edge.getInputVertex().getVertexName()).append(",");
			}
			
			LOG.info("send json to yarn client: " + nodeJson.toString() + 
					"for input " + input +
					" output " + output +
					" parents " + parents.toString());	
			
			//exec.execute(new YarnClientRunnable(nodeJson.toString(), input, 
					//output, parents.toString()));
			
		}
		
	}
	
	private DAG convertToDag(String json) throws JSONException {
		
		DAG dag = DAG.create("dag");
		
		JSONObject jsonObject = new JSONObject(json);
		JSONArray jsonArray = jsonObject.getJSONArray("nodes");
		
		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		for(int i=0;i<jsonArray.length();i++){
			JSONObject nodeJson = jsonArray.getJSONObject(i);
			int id = nodeJson.getInt("id");
			
			String inputTopic = "topic-";
			String outputTopic = "topic-";
			int level = 0;
			if(map.containsKey(id)){
				level = map.get(id);
			}else{
				level = 0;
			}
			inputTopic = inputTopic + level;
			outputTopic = outputTopic + (level+1);
			
			String name = nodeJson.getString("name");
			Vertex vertex = null;
			if(dag.containsVertex(id)){
				vertex = dag.getVertex(id);
			}else{
				vertex = Vertex.create(id);
				dag.addVertex(vertex);
			}	
			vertex.setVertexName(name);
			JSONArray children = nodeJson.getJSONArray("children");
			for(int j=0;j<children.length();j++){
				map.put(children.getInt(j), level+1);
				int cid = children.getInt(j);
				Vertex child = null;
				if(dag.containsVertex(cid)){
					child = dag.getVertex(cid);
				}else{
					child = Vertex.create(cid);
					dag.addVertex(child);
				}
				Edge edge = Edge.create(vertex, child);
				dag.addEdge(edge);
			}
			
			EsperKafkaInput input = new EsperKafkaInput(inputTopic);
			EsperKafkaOutput output = new EsperKafkaOutput(outputTopic);
			
			vertex.setDataSource(input);
			vertex.setDataSink(output);
			
			EsperKafkaProcessor processor = new EsperKafkaProcessor();
			JSONArray eventTypes = nodeJson.getJSONArray("event_types");
			for(int j=0;j<eventTypes.length();j++){
				JSONObject eventType = eventTypes.getJSONObject(j);
				String eventName = eventType.getString("event_type");
				EventType type = new EventType(eventName);
				JSONArray props = eventType.getJSONArray("event_props");
				for(int k=0;k<props.length();k++){
					type.addProp(props.getString(k));
				}
				JSONArray classes = eventType.getJSONArray("event_classes");
				for(int k=0;k<classes.length();k++){
					type.addClass(classes.getString(k));
				}
				processor.addEventType(type);
			}
			JSONArray epls = nodeJson.getJSONArray("epl");
			for(int j=0;j<epls.length();j++){
				processor.addEpl(epls.getString(j));
			}
			processor.setOutType(nodeJson.getString("out_type"));
			
			vertex.setProcessor(processor);
			
		}
		
		return dag;
		
	}
	
	private class YarnClientRunnable implements Runnable {
		
		private String nodeJson;
		private String input;
		private String output;
		private String parents;
		private EsperYarnClient client;
		
		public YarnClientRunnable(String nodeJson, String input, String output, String parents) {
			this.nodeJson = nodeJson;
			this.input = input;
			this.output = output;
			this.parents = parents;
			
			client = new EsperYarnClient();
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			client.runYarnClient(nodeJson, input, output, parents);
		}
		
	}
	
	private class KafkaRunnable implements Runnable {
		
		private KafkaClient<String, String> client;
		
		public KafkaRunnable() {
			client = new KafkaClient<String, String>();
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			Random random = new Random();
			
			try {
				BufferedReader br = new BufferedReader(new FileReader("src/test/java/allData.txt"));
				for(int i=0;i<10;i++){
					String s = br.readLine();
					String[] str = s.split("\t");
					SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS+00:00");
					Date date = simpleDateFormat.parse(str[0]);
					long ts = date.getTime();
					
					StringBuilder eventJson = new StringBuilder();
					eventJson.append("{");
					eventJson.append("\"event_type\":" + "\"CDataPoint\",");
					eventJson.append("\"ts\":" + ts + ",");
					eventJson.append("\"index\":\"" + str[1] + "\",");
					eventJson.append("\"mf01\":\"" + str[2] + "\",");
					eventJson.append("\"mf02\":\"" + str[3] + "\",");
					eventJson.append("\"mf03\":\"" + str[4] + "\",");
					eventJson.append("\"pc13\":\"" + str[5] + "\",");
					eventJson.append("\"pc14\":\"" + str[6] + "\",");
					eventJson.append("\"pc15\":\"" + str[7] + "\",");
					eventJson.append("\"pc25\":\"" + str[8] + "\",");
					eventJson.append("\"pc26\":\"" + str[9] + "\",");
					eventJson.append("\"pc27\":\"" + str[10] + "\",");
					eventJson.append("\"bm05\":" + random.nextInt(2) + ",");
					eventJson.append("\"bm06\":" + random.nextInt(2) + ",");
					eventJson.append("\"bm07\":" + random.nextInt(2) + ",");
					eventJson.append("\"bm08\":" + random.nextInt(2) + ",");
					eventJson.append("\"bm09\":" + random.nextInt(2) + ",");
					eventJson.append("\"bm10\":" + random.nextInt(2) + "");
					eventJson.append("}");
					
					client.produce(i+"", eventJson.toString(), "topic-0");
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
				client.produce(null, "{\"event_type\":\"quit\",\"quit\":\"quit\"}", "topic-0");
				client.close();
			}
			
			//test();
			
		}
		
		private void test() {
			/*client.produce(null, 
					"{\"event_type\":\"person_event\", \"age\":100, \"name\":\"testname1\"}", 
					"topic_0");
			client.produce(null, 
					"{\"event_type\":\"person_event\", \"age\":200, \"name\":\"testname2\"}", 
					"topic_0");*/
			client.produce(null, "{\"event_type\":\"quit\",\"quit\":\"quit\"}", "topic-0");
			client.close();
		}
		
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SpringApplication.run(DAGController.class, args);

	}

}
