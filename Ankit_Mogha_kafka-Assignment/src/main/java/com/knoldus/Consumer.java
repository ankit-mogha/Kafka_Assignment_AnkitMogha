package com.knoldus;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {
    
    public static void main(String[] args) {
       ConsumerListener c = new ConsumerListener();
        Thread thread = new Thread(c);
        thread.start();
    }
      public static void consumer() throws IOException
      {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("max.block.ms",1000);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "com.knoldus.UserDeserializer");
        properties.put("group.id", "test-group");

        FileWriter file = new FileWriter("MessagesList.txt");
        KafkaConsumer<String, User> kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("user");
        kafkaConsumer.subscribe(topics);

        try{
            while (true)
            {
                ConsumerRecords<String, User> records = kafkaConsumer.poll(1000);
                for (ConsumerRecord<String, User> record: records)
                {
                    System.out.println(record.value());
                    file.write(record.value() + "\n");
                    file.flush();
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
        }
    }
}

class ConsumerListener implements Runnable
{
    public void run()
    {
        try {
            Consumer.consumer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
