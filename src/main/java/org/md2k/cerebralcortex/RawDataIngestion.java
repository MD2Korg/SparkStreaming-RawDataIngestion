package org.md2k.cerebralcortex;

/*
 * Copyright (c) 2016, The University of Memphis, MD2K Center
 * - Timothy Hnat <twhnat@memphis.edu>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.md2k.cerebralcortex.cassandra.DataPoint;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

/*
Cassandra Init:

CREATE KEYSPACE cc_SITE_production WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;

CREATE TABLE cc_SITE_production.data (
    datastream_identifier uuid,
    day varchar,
    starttime timestamp,
    endtime timestamp,
    offset smallint,
    sample varchar,
    PRIMARY KEY ((datastream_id, day), starttime, endtime)
) WITH CLUSTERING ORDER BY (starttime ASC);

 CREATE TABLE cc_SITE_production.datastreams (

    participant_identifier uuid,
    datastream_identifier uuid,

    datadescriptor varchar,

    datasource_identifier varchar,
    datasource_type varchar,
    datasource_metadata varchar,

    application_identifier varchar,
    application_type varchar,
    application_metadata varchar,

    platform_identifier varchar,
    platform_type varchar,
    platform_metadata varchar,

    platformapp_identifier varchar,
    platformapp_type varchar,
    platformapp_metadata varchar,

    PRIMARY KEY (participant_id, datastream_id)
 );


 CREATE TABLE cc_SITE_production.studies (
    study_identifier varchar,
    study_name varchar,
    participant_identifier uuid,
    participant_name varchar,
    PRIMARY KEY (study_identifier, participant_identifier)
 );

//TWH: These need work, but are in the right direction
 create materialized view studies_by_name
 as select study_identifier, study_name, participant_identifier, participant_name
 from studies
 where study_identifier is not null and study_name is not null
 primary key (study_identifier, study_name)
 with clustering order by (study_name asc);

create materialized view studies_by_participant
 as select study_identifier, study_name, participant_identifier, participant_name
 from studies
 where study_identifier is not null and participant_name is not null
 primary key (study_identifier, participant_name)
 with clustering order by (participant_name asc);


 CREATE TABLE cc_SITE_production.count (
    datastream_id uuid,
    day varchar,
    label varchar,
    count counter,
    PRIMARY KEY ((datastream_id, day), label)
 ) with clustering order by (label asc);




 */



/**
 * Main class that implements a Spark-Streaming routine to extract Kafka messages that contain raw data readings and
 * persist them appropriately in a Cassandra data store.
 */
public final class RawDataIngestion {

    private static final String CASSANDRA_HOST = "tatooine10dot";
    private static final String SPARK_MASTER = "spark://tatooine10dot:7077";
    private static final int INTERVAL_TIME = 5;
    private static final String BROKER_HOST = "tatooine10dot:9092";
    private static final String CEREBRALCORTEX_TABLE = "rawdata";
    private static final String APP_NAME = "RawDataIngestion-";
    private static String CEREBRALCORTEX_KEYSPACE = "";

    /**
     * Main driver entry point
     *
     * @param args Unused
     */
    public static void main(String[] args) {
        // Setup Kafka topics to listen to
        //        String topics = "RAILS-bulkload";
        if (args.length < 1) {
            System.err.println("Missing Cassandra Keyspace: e.g. cerebralcortex");
            System.err.println("Missing Kafka BulkLoad Topic: e.g. DATABASE-RAILS-bulkload");
            return;
        }
        String topics = args[1];
        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

        CEREBRALCORTEX_KEYSPACE = args[0];

        // Configure Spark and Java contexts
        SparkConf conf = new SparkConf()
                .set("spark.cassandra.connection.host", CASSANDRA_HOST)
                .setMaster(SPARK_MASTER)
                .set("spark.cores.max", "4")
                .setAppName(APP_NAME + CEREBRALCORTEX_KEYSPACE);
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(INTERVAL_TIME));


        // Configure kafka connection
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", BROKER_HOST);
//        kafkaParams.put("auto.offset.reset", "smallest");

        // Listen for Kafka messages
        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(streamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

        // Extract the values which contain JSON-encoded data from each tuple
        JavaDStream<String> lines = directKafkaStream.map(
                new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String>tuple2) throws Exception {
                        return tuple2._2();
                    }
                }
        );

        // Parse each entry into a JsonObject
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

        // Extract and create individual datapoints from each of the JSON arrays
        JavaDStream<DataPoint> datapoints = jsonData.flatMap(
                new FlatMapFunction<JsonObject, DataPoint>() {
                    @Override
                    public Iterable<DataPoint> call(JsonObject jsonObject) throws Exception {
                        List<DataPoint> result = new ArrayList<>();
                        for(JsonElement jo: jsonObject.get("data").getAsJsonArray()){
                            DataPoint dp = new DataPoint(
                                    jsonObject.get("datastream_id").getAsInt(),
                                    new SimpleDateFormat("yyyyMMdd").format(new Date(jo.getAsJsonObject().get("dateTime").getAsLong())),
                                    new Date(jo.getAsJsonObject().get("dateTime").getAsLong()),
                                    jo.getAsJsonObject().get("offset").getAsInt() / 60000,
                                    jo.getAsJsonObject().get("sample").toString()
                            );

                            result.add(dp);
                        }
                        return result;
                    }
                }

        );

        // Write datapoints to Cassandra
        javaFunctions(datapoints).writerBuilder(CEREBRALCORTEX_KEYSPACE, CEREBRALCORTEX_TABLE, mapToRow(DataPoint.class)).saveToCassandra();

        // Extract samples as a string for debug logs
        JavaDStream<String> result = datapoints.map(
                new Function<DataPoint, String>() {
                    @Override
                    public String call(DataPoint v1) throws Exception {
                        return v1.getSample();
                    }
                }
        );

        // Print out 10 entries in the log for debugging if needed
        result.print();

        // Start the streaming context and run forever
        streamingContext.start();
        streamingContext.awaitTermination();

    }

}
