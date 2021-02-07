package com.knoldus;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class Producer {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("max.block.ms",1000);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.knoldus.UserSerializer");
        
        KafkaProducer kafkaProducer = new KafkaProducer<String, User>(properties);
        List<User> users = new ArrayList<>();
        Random rand = new Random();
        for(int i=1; i<=10; i++) {
            users.add(new User(i, "Ankit", rand.nextInt((30 - 10) + 1) + 10, "MCA"));
        }

        try{
            for(User user : users){
                System.out.println(user);
                kafkaProducer.send(new ProducerRecord<String, User>("user",user.toString(),user));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}