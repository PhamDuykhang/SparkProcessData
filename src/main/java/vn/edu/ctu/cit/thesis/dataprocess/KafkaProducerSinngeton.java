package vn.edu.ctu.cit.thesis.dataprocess;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class KafkaProducerSinngeton {
    private static KafkaProducer producer = null;
    private KafkaProducerSinngeton(){
    }
    public static KafkaProducer getInstant(){
        if (producer == null){
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("acks","all");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer(props);
        }
        return producer;
    }
}
