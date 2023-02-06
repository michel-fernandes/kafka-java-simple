import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Consumer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties());

        consumer.subscribe(Collections.singletonList("compras.do.cliente"));
        while (true){
            //var List<> records =
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord record: records){
                System.out.print("Compra nova: ");
                System.out.print(record.key() + "");
                System.out.print(record.value() + " ");
                System.out.print(record.offset() + " ");
                System.out.println(record.partition());
            }
        }

    }
    private static Properties properties() {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumo-cliente");
        return properties;
    }
}
