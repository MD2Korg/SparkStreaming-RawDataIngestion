import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import kafka.serializer.StringDecoder;
import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.*;
import scala.Tuple2;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.*;


public class RawDataIngestion {
    public static void main(String [] args) {
        SparkConf conf = new SparkConf().set("spark.cassandra.connection.host", "tatooine10dot").setMaster("spark://tatooine10dot:7077").setAppName("SparkStreamingMain");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        String topics = "RAILS-bulkload";
        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "tatooine10dot:9092");
//        kafkaParams.put("auto.offset.reset", "smallest");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(streamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

        JavaDStream<String> lines = directKafkaStream.map(
                new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String>tuple2) throws Exception {
                        return tuple2._2();
                    }
                }
        );

        JavaDStream<JsonObject> jsonData = lines.map(
                new Function<String, JsonObject>() {
                    @Override
                    public JsonObject call(String v1) throws Exception {
                        JsonObject result = new JsonObject();
                        try {
                            result = new JsonParser().parse(v1).getAsJsonObject();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return result;
                    }
                }
        );


        JavaDStream<DataPoint> datapoints = jsonData.flatMap(
                new FlatMapFunction<JsonObject, DataPoint>() {
                    @Override
                    public Iterable<DataPoint> call(JsonObject jsonObject) throws Exception {
                        List<DataPoint> result = new ArrayList<>();
                        for(JsonElement jo: jsonObject.get("data").getAsJsonArray()){
                            DataPoint dp = new DataPoint(
                                    jsonObject.get("datastream_id").getAsInt(),
                                    new SimpleDateFormat("yyyymmdd").format(new Date(jo.getAsJsonObject().get("dateTime").getAsLong())),
                                    new Date(jo.getAsJsonObject().get("dateTime").getAsLong()),
                                    jo.getAsJsonObject().get("offset").getAsInt(),
                                    jo.getAsJsonObject().get("sample").toString()
                            );

                            result.add(dp);
                        }
                        return result;
                    }
                }

        );


        javaFunctions(datapoints).writerBuilder("cerebralcortex", "rawdata", mapToRow(DataPoint.class)).saveToCassandra();

        JavaDStream<String> result = datapoints.map(
                new Function<DataPoint, String>() {
                    @Override
                    public String call(DataPoint v1) throws Exception {
                        return v1.getSample();
                    }
                }
        );

        result.print();


        streamingContext.start();
        streamingContext.awaitTermination();
//        streamingContext.awaitTermination(5000);

    }

    public static class DataPoint implements Serializable{
        private Integer datastream_id;
        private String day;
        private Date datetime;
        private Integer offset;
        private String sample;

        public DataPoint() {}

        public DataPoint(Integer datastream_id, String day, Date datetime, Integer offset, String sample) {
            this.datastream_id = datastream_id;
            this.day = day;
            this.datetime = datetime;
            this.offset = offset;
            this.sample = sample;
        }

        public Integer getDatastream_id() {
            return datastream_id;
        }

        public void setDatastream_id(Integer datastream_id) {
            this.datastream_id = datastream_id;
        }

        public String getDay() {
            return day;
        }

        public void setDay(String day) {
            this.day = day;
        }

        public Date getDatetime() {
            return datetime;
        }

        public void setDatetime(Date datetime) {
            this.datetime = datetime;
        }

        public Integer getOffset() {
            return offset;
        }

        public void setOffset(Integer offset) {
            this.offset = offset;
        }

        public String getSample() {
            return sample;
        }

        public void setSample(String sample) {
            this.sample = sample;
        }
    }
}
