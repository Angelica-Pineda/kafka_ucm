package com.farmia.streaming;

import com.farmia.sales.SalesSummary;
import com.farmia.sales.SalesTransaction;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStore;

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

        // Serdes para entrada y salida
        SpecificAvroSerde<SalesTransaction> salesSerde = new SpecificAvroSerde<>();
        salesSerde.configure(serdeConfig, false);

        SpecificAvroSerde<SalesSummary> summarySerde = new SpecificAvroSerde<>();
        summarySerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Stream de entrada (agrupamos por categoría)
        KStream<String, SalesTransaction> sales = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), salesSerde));

        sales
                // .toString() soluciona el error de CharSequence (Error 1)
                .selectKey((key, value) -> value.getCategory().toString())

                .groupByKey(Grouped.with(Serdes.String(), salesSerde))

                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))

                // Especificamos los tipos <String, Double, WindowStore> para que no sea "Object" (Error 3)
                .aggregate(
                        () -> 0.0,
                        (key, value, aggregate) -> aggregate + value.getPrice(),
                        Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("sales-aggregation-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double())
                )

                .toStream()
                // Forzamos el mapeo a String y SalesSummary (Soluciona Error 4)
                .map((Windowed<String> windowedKey, Double total) -> {
                    SalesSummary summary = SalesSummary.newBuilder()
                            .setCategory(windowedKey.key())
                            .setTotalSales(total)
                            .setWindowStart(windowedKey.window().start())
                            .setWindowEnd(windowedKey.window().end())
                            .build();
                    return new KeyValue<>(windowedKey.key(), summary);
                })
                .peek((key, summary) -> System.out.println("Venta detectada en " + key + ": " + summary.getTotalSales()))
                .to(outputTopic, Produced.with(Serdes.String(), summarySerde));

        return builder.build();
    }

    public static void main(String[] args) throws IOException {
        Properties props = ConfigLoader.getProperties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sales-summary-app");

        KafkaStreams streams = new KafkaStreams(createTopology(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}