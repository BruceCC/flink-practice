package org.leave.flink.practice.goods.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/6
 */
@Slf4j
public class File2KafkaUtil {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "hotitems";
        String filePath = "D:/workspace/test-data/UserBehavior/UserBehavior.csv";
        //String filePath = "data/sample.csv";
        file2Kafka(bootstrapServers, topic, filePath);
    }

    public static void file2Kafka(String bootstrapServers, String topic, String filePath) {
        //文件抽取计时
        long start = System.currentTimeMillis();

        //kafka配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        try {
            File file = new File(filePath);
            BufferedReader bufferReader = new BufferedReader(new FileReader(file));
            String line;
            log.info("begin read data from file...");
            while ((line = bufferReader.readLine()) != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
                log.debug("send to kafka, data: {}", line);
                kafkaProducer.send(record).get();
            }

            bufferReader.close();
            kafkaProducer.close();
            long end = System.currentTimeMillis();
            log.info("File data to kafka finished, elapsed time " + (end -start)/1000 + "s");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
