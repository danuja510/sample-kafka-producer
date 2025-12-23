package org.example;

import static java.lang.System.*;

import java.util.*;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public final class BasicProducerSample {
    private static final String[] MANIPULATION_TYPES = {"U", "D", "C"};
    private static final RecordType[] RECORD_TYPES = RecordType.values();

    public static void main(String[] args) throws InterruptedException {
        final var topic = "sample-topic";
        final var gson = new Gson();
        final var random = new Random();

        final Map<String, Object> config =
                Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try (var producer = new KafkaProducer<String, String>(config)) {
            for (int i = 0; i < 10000; i++) {
                // Generate random warehouse ID (001-100 with leading zeros)
                String warehouseId = String.format("WH-%03d", random.nextInt(100) + 1);

                // Generate random record type
                RecordType recordType = RECORD_TYPES[random.nextInt(RECORD_TYPES.length)];

                // Generate random value (between 1000.0 and 999999.0)
                double value = 1000.0 + (random.nextDouble() * 998999.0);

                // Create the record object
                Record record = new Record(
                        warehouseId,
                        recordType.name(),
                        value
                );

                // Generate random manipulation type
                String manipulationType = MANIPULATION_TYPES[random.nextInt(MANIPULATION_TYPES.length)];

                // Create the Kafka message
                KafkaMessage message = new KafkaMessage(
                        manipulationType,
                        "localhost:9092",
                        record
                );

                // Convert to JSON
                final var jsonValue = gson.toJson(message);
                out.format("Publishing record with value %s%n", jsonValue);

                final Callback callback = (metadata, exception) -> {
                    out.format("Published with metadata: %s, error: %s%n",
                            metadata, exception);
                };

                // publish the record with null key for round-robin load balancing across partitions
                producer.send(new ProducerRecord<>(topic, null, jsonValue), callback);

                // wait a second before publishing another
                Thread.sleep(10);
            }
        }
    }
}