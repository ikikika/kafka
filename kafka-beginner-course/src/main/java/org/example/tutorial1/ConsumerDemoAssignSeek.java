package org.example.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        String bootstrapServers = "localhost:9092";
//        String groupId = "fourth_app";
        String topic = "first_topic";

        // step 1: create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // step 2: create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // assign and seek are mostly used to replay data or fetch a specific message
        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offSetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));
        // seek
        consumer.seek(partitionToReadFrom, offSetToReadFrom);

        // step 3: subscribe consumer to topic(s)
//        consumer.subscribe(Arrays.asList(topic));

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        // step 4: oll for new data
        while( keepOnReading ){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for ( ConsumerRecord record: records ){
                numberOfMessagesReadSoFar += 1;
                logger.info("Key: " + record.key() + ", Value:" + record.value() );
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset() );
                if( numberOfMessagesReadSoFar >= numberOfMessagesToRead ){
                    keepOnReading = false; // exit while loop
                    break; // exit for loop
                }
            }
        }

        logger.info("Exiting the application");
    }
}
