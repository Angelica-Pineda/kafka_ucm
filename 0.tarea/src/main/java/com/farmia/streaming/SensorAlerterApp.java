
package com.farmia.streaming;

import com.farmia.iot.SensorTelemetry;
import com.farmia.iot.SensorAlert;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SensorAlerterApp {

    private static Topology createTopology() {
        final String inputTopic = "sensor-telemetry";
        final String outputTopic = "sensor-alerts";
        final String schemaRegistryUrl = "http://localhost:8081";

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistryUrl);

        SpecificAvroSerde<SensorTelemetry> sensorSerde = new SpecificAvroSerde<>();
        sensorSerde.configure(serdeConfig, false);

        SpecificAvroSerde<SensorAlert> alertSerde = new SpecificAvroSerde<>();
        alertSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, SensorTelemetry> sensors = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), sensorSerde));

        sensors
                .filter((key, val) -> val.getTemperature() > 35 || val.getHumidity() < 20)
                .mapValues(val -> {
                    String type;
                    String details;

                    if (val.getTemperature() > 35) {
                        type = "HIGH_TEMPERATURE";
                        details = "Temperature exceed 35ºC";
                    } else {
                        type = "LOW_HUMIDITY";
                        details = "humidity below 20%";
                    }

                    return SensorAlert.newBuilder()
                            .setSensorId(val.getSensorId())
                            .setAlertType(type)
                            .setTimestamp(System.currentTimeMillis())
                            .setDetails(details)
                            .build();
                })
                .peek((key, alert) -> System.out.println("Alert generated: " + alert.getAlertType() + " for " + alert.getSensorId()))
                .to(outputTopic, Produced.with(Serdes.String(), alertSerde));

        return builder.build();
    }

    public static void main(String[] args) throws IOException {
        Properties props = ConfigLoader.getProperties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sensor-alerter-app");

        KafkaStreams streams = new KafkaStreams(createTopology(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
