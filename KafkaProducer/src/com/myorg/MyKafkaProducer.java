package com.myorg;

import com.myorg.util.PropertiesLoader;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyKafkaProducer {
	
	public static void main(String[] args) throws InterruptedException {
		if (args.length != 2) {
			System.out.println("Usage: com.myorg.MyKafkaProducer <topic_name> limit");
			System.exit(-1);
		}

		String topic = args[0];
		String groupId = "java-app";
		int pause = 0;
		if(args.length == 3){
			pause = Integer.valueOf(args[2]);
		}
		int limit = Integer.valueOf(args[1]);
		Properties props = PropertiesLoader.getKafkaProperties(groupId);
		Producer<String, String> producer = new KafkaProducer<String, String>(
				props);
		int i = 0;
		while (i < limit){
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, null, Integer.toString(i));
			producer.send(record);
			System.out.println("Sending " + i);
			++i;
			Thread.sleep(pause);
		}
		System.out.println("Messages were sent successfully");
		producer.close();
	}
}
