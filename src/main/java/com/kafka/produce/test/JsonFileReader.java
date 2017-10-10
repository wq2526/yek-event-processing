package com.kafka.produce.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.csvreader.CsvReader;

public class JsonFileReader {
	
	public String readJson() {
		
		StringBuilder eventJson = new StringBuilder();
		
		try {
			BufferedReader in = new BufferedReader(new FileReader("src/test/java/test.json"));
			String s = in.readLine();
			while(s!=null){
				eventJson.append(s + "\n");
				s = in.readLine();
			}
			
			in.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(eventJson.toString());
		
		return eventJson.toString();
		
	}
	
	public void readCSV() {
		
		try {
			CsvReader reader = new CsvReader("src/test/java/test.csv");
			
			reader.readHeaders();
			int i = 0;
			
			while(reader.readRecord()){
				StringBuilder eventJson = new StringBuilder();
				eventJson.append("{");
				eventJson.append("\"city\":\"" + reader.get("city") + "\",");
				eventJson.append("\"local\":\"" + reader.get("local") + "\",");
				eventJson.append("\"parameter\":\"" + reader.get("parameter") + "\",");
				eventJson.append("\"value\":\"" + reader.get("value") + "\"");
				eventJson.append("}");
				
				System.out.println(i++ + eventJson.toString());
				
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		JsonFileReader jfr = new JsonFileReader();
		jfr.readCSV();

	}

}
