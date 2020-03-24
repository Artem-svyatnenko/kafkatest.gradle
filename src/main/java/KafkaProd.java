import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


//Класс-оболочка для KafkaProducer
public class KafkaProd {

    private Producer<String, String> producer;
    private String topicName;

    public KafkaProd() {
        System.out.println("Создание Kafka Producer для топика test...");
        //Название топика в Kafka
        topicName = "test";

        //Настройки для продюсера
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                StringSerializer.class.getName());
        props.put("value.serializer",

                StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);
        System.out.println("Kafka Producer успешно создан");
    }


    public void sendMessage(String message) {

        producer.send(new ProducerRecord<String, String>(topicName, message));
    }

    public void close() {

        producer.close();
    }
}
