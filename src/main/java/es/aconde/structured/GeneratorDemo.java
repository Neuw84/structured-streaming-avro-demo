package es.aconde.structured;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.SplittableRandom;
import java.util.Properties;

/**
 * Fake data generator for Kafka
 *
 * @author Angel Conde
 */
public class GeneratorDemo {

    /**
     * Avro defined schema
     */
    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"alarm\","
            + "\"fields\":["
            + "  { \"name\":\"str1\", \"type\":\"string\" },"
            + "  { \"name\":\"str2\", \"type\":\"string\" },"
            + "  { \"name\":\"int1\", \"type\":\"int\" },"
            + "  { \"name\": \"packet_info\", \"type\": {"
            + "        \"type\": \"record\","
            + "        \"name\": \"packet_data\","
            + "        \"fields\": ["
            + "              { \"name\": \"demo\", \"type\": \"string\" }"
            + "          ]}}"
            + "]}";

    /**
     *
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);
        System.out.println(schema.toString(true));
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        SplittableRandom random = new SplittableRandom();
        int count = 0;
        while (true) {
            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("str1", "Str 1-" + random.nextInt(10));
            avroRecord.put("str2", "Str 2-" + random.nextInt(1000));
            avroRecord.put("int1", random.nextInt(10000));
            GenericData.Record packetInfo = new GenericData.Record(schema.getField("packet_info").schema());
            packetInfo.put("demo", "value");
            avroRecord.put("packet_info", packetInfo);
            byte[] bytes = recordInjection.apply(avroRecord);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("mytopic", bytes);
            producer.send(record);
            count++;
            if (count % 300000 == 0) {
                Thread.sleep(500);
            }
        }

    }
}
