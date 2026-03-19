package com.farmia.streaming;

import com.farmia.sales.SalesSummary;
import com.farmia.sales.sales_transactions;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SalesSummaryApp {

    private static Topology createTopology() {
        final String inputTopic = "sales-transactions";
        final String outputTopic = "sales-summary";
        final String schemaRegistryUrl = "http://localhost:8081";

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistryUrl);

        SpecificAvroSerde<sales_transactions> salesSerde = new SpecificAvroSerde<>();
        salesSerde.configure(serdeConfig, false);

        SpecificAvroSerde<SalesSummary> summarySerde = new SpecificAvroSerde<>();
        summarySerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, sales_transactions> sales = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), salesSerde));

        sales
                .groupByKey(Grouped.with(Serdes.String(), salesSerde))

                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))

                .aggregate(
                        () -> 0.0,
                        (key, value, aggregate) -> aggregate + value.getPrice(),
                        Materialized.with(Serdes.String(), Serdes.Double())
                )

                .toStream()
                .map((windowedKey, total) -> {
                    return KeyValue.pair(windowedKey.key(),
                            SalesSummary.newBuilder()
                                    .setCategory(windowedKey.key())
                                    .setTotalSales(total)
                                    .setWindowStart(windowedKey.window().start())
                                    .setWindowEnd(windowedKey.window().end())
                                    .build());
                })
                .peek((key, summary) -> System.out.println("Categoría: " + key + " | Total Ventas: " + summary.getTotalSales()))

                .to(outputTopic, Produced.with(Serdes.String(), summarySerde));

        return builder.build();
    }

    public static void main(String[] args) throws IOException {
        Properties props = ConfigLoader.getProperties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sales-summary-app-v6");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "C:/kafka-temp/sales-summary-v6");

        KafkaStreams streams = new KafkaStreams(createTopology(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}