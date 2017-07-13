package es.aconde.structured;

import com.databricks.spark.avro.SchemaConverters;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Structured streaming demo using Avro'ed Kafka topic as input
 *
 * @author Angel Conde
 */
public class StructuredDemo {

    private static Injection<GenericRecord, byte[]> recordInjection;
    private static StructType type;
    private static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"str1\", \"type\":\"string\" },"
            + "  { \"name\":\"str2\", \"type\":\"string\" },"
            + "  { \"name\":\"int1\", \"type\":\"int\" }"
            + "]}";
    private static Schema.Parser parser = new Schema.Parser();
    private static Schema schema = parser.parse(USER_SCHEMA);

    static { //once per VM, lazily
        recordInjection = GenericAvroCodecs.toBinary(schema);
        type = (StructType) SchemaConverters.toSqlType(schema).dataType();

    }

    public static void main(String[] args) throws StreamingQueryException {
        //set log4j programmatically
        LogManager.getLogger("org.apache.spark").setLevel(Level.WARN);
        LogManager.getLogger("akka").setLevel(Level.ERROR);

        //configure Spark
        SparkConf conf = new SparkConf()
                .setAppName("kafka-structured")
                .setMaster("local[*]");

        //initialize spark session
        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        //reduce task number
        sparkSession.sqlContext().setConf("spark.sql.shuffle.partitions", "3");

        //data stream from kafka
        Dataset<Row> ds1 = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "mytopic")
                .option("startingOffsets", "earliest")
                .load();

        //start the streaming query
        sparkSession.udf().register("deserialize", (byte[] data) -> {
            GenericRecord record = recordInjection.invert(data).get();
            return RowFactory.create(record.get("str1").toString(), record.get("str2").toString(), record.get("int1"));

        }, DataTypes.createStructType(type.fields()));
        ds1.printSchema();
        Dataset<Row> ds2 = ds1
                .select("value").as(Encoders.BINARY())
                .selectExpr("deserialize(value) as rows")
                .select("rows.*");

        ds2.printSchema();

        StreamingQuery query1 = ds2
                .groupBy("str1")
                .count()
                .writeStream()
                .queryName("Test query")
                .outputMode("complete")
                .format("console")
                .start();

        query1.awaitTermination();

    }
}
