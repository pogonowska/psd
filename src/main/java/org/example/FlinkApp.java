package org.example;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import org.apache.flink.util.Collector;

import static org.example.FlinkConsumer.createStringConsumerForTopic;
import static org.example.FlinkProducer.createStringProducer;

public class FlinkApp {

    public static void main(String[] args) throws Exception {
        String inputTopic = "transactions";
        String outputTopic = "alarm";
        String consumerGroup = "Combined Lag";
        String address = "localhost:9092";

        StreamExecutionEnvironment environment = StreamExecutionEnvironment
                .getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        FlinkKafkaConsumer<InputMessage> flinkKafkaConsumer = createStringConsumerForTopic(
                inputTopic, address, consumerGroup);
        DataStream<InputMessage> inputStream = environment.addSource(flinkKafkaConsumer);

        //////////////////////////////////////////////////////////////////////////////////////////////////////
        //BIG VALUE
        DataStream<OutputMessage> bigValueAlert = inputStream
                .filter(
                        new FilterFunction<InputMessage>() {
                            @Override
                            public boolean filter(InputMessage message) throws Exception {
                                return message.value > 10000;
                            }
                        })
                .map(new MapFunction<InputMessage, OutputMessage>() {
                    @Override
                    public OutputMessage map(InputMessage message) throws Exception {
                        return new OutputMessage(message, "Alert Type: Large value of transaction");
                    }
                });

        //////////////////////////////////////////////////////////////////////////////////////////////////////
        //LOCATION

        DataStream<OutputMessage> locationAlert = inputStream
                .filter(
                        new FilterFunction<InputMessage>() {
                            @Override
                            public boolean filter(InputMessage message) throws Exception {
                                return message.latitude > 55 || message.latitude < 48 ||
                                        message.longitude > 25 || message.longitude < 13;
                            }
                        })
                .map(new MapFunction<InputMessage, OutputMessage>() {
                    @Override
                    public OutputMessage map(InputMessage message) throws Exception {
                        return new OutputMessage(message, "Alert Type: Unusual place of transaction");
                    }
                });

        //////////////////////////////////////////////////////////////////////////////////////////////////////
        //TOOOFTEN
        SingleOutputStreamOperator<Tuple2<String, Integer>> oftenAlert = inputStream
                .flatMap(
                        new FlatMapFunction<InputMessage, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(InputMessage message, Collector<Tuple2<String, Integer>> out) {
                                // Zlicz wystąpienia obiektów i wyemituj krotki
                                out.collect(new Tuple2<>(Integer.toString(message.card_id), 1));
                            }
                        })
                .keyBy(0)
                .timeWindow(Time.seconds(10)) // Okno czasowe 10 sekund
                .sum(1);

        DataStream<OutputMessage> oftenAlertFiltered = oftenAlert
                .filter(tuple -> tuple.f1 > 3)
                .map(new MapFunction<Tuple2<String, Integer>, OutputMessage>() {
                    @Override
                    public OutputMessage map(Tuple2<String, Integer> message) throws Exception {
                        return new OutputMessage(message, "Alert Type: Too Often");
                    }
                });

        FlinkKafkaProducer<OutputMessage> flinkKafkaProducer = createStringProducer(
                outputTopic, address);
        FlinkKafkaProducer<OutputMessage> flinkKafkaProducer2 = createStringProducer(
                outputTopic, address);
        FlinkKafkaProducer<OutputMessage> flinkKafkaProducer3 = createStringProducer(
                outputTopic, address);
        bigValueAlert.addSink(flinkKafkaProducer);
        locationAlert.addSink(flinkKafkaProducer2);
        oftenAlertFiltered.addSink(flinkKafkaProducer3);
        environment.execute();
    }
}
