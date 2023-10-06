package org.iHarwood;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.bson.Document;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Archiver {
    private static String collectionName = "gcn";
    private String topicName = "citi.GCN";
    private String consumerID = "ARCHIVER";


    public String topicName() {
        return this.topicName;
    }

    public String consumerID() {
        return this.consumerID;
    }

    /*@Bean
    public String setConsumerID(String consumerID) {
        this.consumerID = consumerID;

        return this.consumerID;
    }*/

    public boolean Runner(String topicName, String consumerID) {
        MongoClient mongoClient;
        MongoDatabase database;

        try {
            // Open database
            mongoClient = MongoClients.create("mongodb://localhost:27017");
            database = mongoClient.getDatabase("gcnDB");
            MongoCollection<Document> messageCollection = database.getCollection(collectionName);

            // Open Kafka
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", consumerID);
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

            //Kafka Consumer subscribes list of topics here.
            //consumer.subscribe(Arrays.asList(topicName));
            consumer.subscribe(Collections.singletonList(topicName));

            //print consumer and topic name
            System.out.println("Consumer: " + consumerID);
            System.out.println("Topic: " + topicName);
            String keyValue;

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {

                    JSONObject jsonObject = new JSONObject();

                    for (Header header : record.headers()) {
                        keyValue = new String(header.value(), StandardCharsets.UTF_8);
                        jsonObject.put(header.key(), keyValue);
                    }

                    jsonObject.put(record.key(), record.value());

                    Document doc = new Document("message", jsonObject.toString());
                    InsertOneResult insertOneResult = messageCollection.insertOne(doc);

                    System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            System.out.println("Database open failed");
        }

        return true;
    }
}
