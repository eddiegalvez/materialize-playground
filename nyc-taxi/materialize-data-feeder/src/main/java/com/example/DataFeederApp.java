package com.example;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main application to feed NYC taxi data from a Parquet file to a Kafka topic.
 * This app uses a producer-consumer pattern with a bounded queue for backpressure.
 */
public class DataFeederApp {

    // --- CONFIGURATION ---
    /* TODO make configurable at invocation */
    private static final String PARQUET_FILE_PATH = "/Users/eddie/code/materialize/nyc-taxi/data/yellow_tripdata_2024-06.parquet";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "yellow-taxi-trips";
    
    // Configuration for the bounded queue
    private static final long QUEUE_MAX_SIZE_MB = 1024;
    private static final int TRIP_RECORD_ROUGH_SIZE_BYTES = 256; 
    private static final int QUEUE_CAPACITY = (int) ((QUEUE_MAX_SIZE_MB * 1024 * 1024) / TRIP_RECORD_ROUGH_SIZE_BYTES);

    // Shared flag to signal completion
    private static final AtomicBoolean isReadingFinished = new AtomicBoolean(false);

    public static void main(String[] args) {
        System.out.println("Starting Data Feeder Application (to Kafka)...");
        System.out.printf("Queue Capacity: %,d records (approximating %,d MB)%n", QUEUE_CAPACITY, QUEUE_MAX_SIZE_MB);

        BlockingQueue<TripRecord> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            
            // TODO proper exception handling
            ParquetReaderTask readerTask = new ParquetReaderTask(PARQUET_FILE_PATH, queue, isReadingFinished, 100);
            var readerFuture = executor.submit(readerTask);

            KafkaProducerTask producerTask = new KafkaProducerTask(queue, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, isReadingFinished);
            var producerFuture = executor.submit(producerTask);

            System.out.println("Waiting for reader to complete...");
            try {
                readerFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("Error from reader task: " + e.getMessage());
                e.printStackTrace();
            }
            

            System.out.println("Reader completed, waiting for kafka producer to complete...");
            try {
                producerFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("Error from producer task: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}

/**
 * A Java 21 Record to represent a single taxi trip.
 */
record TripRecord(
    Integer VendorID,
    LocalDateTime tpep_pickup_datetime,
    LocalDateTime tpep_dropoff_datetime,
    Long passenger_count,
    Double trip_distance,
    Long RatecodeID,
    String store_and_fwd_flag,
    Integer PULocationID, // pickup
    Integer DOLocationID, // dropoff
    Long payment_type,
    Double fare_amount,
    Double extra,
    Double mta_tax,
    Double tip_amount,
    Double tolls_amount,
    Double improvement_surcharge,
    Double total_amount,
    Double congestion_surcharge,
    Double Airport_fee
) {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    public String getKey() {
        return String.valueOf(PULocationID);
    }

    public static TripRecord fromGenericRecord(GenericRecord record) {
        Long pickupMicros = (Long) record.get("tpep_pickup_datetime");
        Long dropoffMicros = (Long) record.get("tpep_dropoff_datetime");

        return new TripRecord(
            (Integer) record.get("VendorID"),
            (pickupMicros != null) ? LocalDateTime.ofInstant(Instant.ofEpochMilli(pickupMicros / 1000), ZoneOffset.UTC) : null,
            (dropoffMicros != null) ? LocalDateTime.ofInstant(Instant.ofEpochMilli(dropoffMicros / 1000), ZoneOffset.UTC) : null,
            (Long) record.get("passenger_count"),
            (Double) record.get("trip_distance"),
            (Long) record.get("RatecodeID"),
            (record.get("store_and_fwd_flag") != null) ? record.get("store_and_fwd_flag").toString() : null,
            (Integer) record.get("PULocationID"),
            (Integer) record.get("DOLocationID"),
            (Long) record.get("payment_type"),
            (Double) record.get("fare_amount"),
            (Double) record.get("extra"),
            (Double) record.get("mta_tax"),
            (Double) record.get("tip_amount"),
            (Double) record.get("tolls_amount"),
            (Double) record.get("improvement_surcharge"),
            (Double) record.get("total_amount"),
            (Double) record.get("congestion_surcharge"),
            (Double) record.get("Airport_fee")
        );
    }
    
   /**
     * Serializes the record to a JSON string using org.json, handling nulls and types safely.
     */
    public String toJson() {
        JSONObject obj = new JSONObject();
        obj.put("VendorID", VendorID != null ? VendorID : JSONObject.NULL);
        obj.put("tpep_pickup_datetime", tpep_pickup_datetime != null ? tpep_pickup_datetime.format(formatter) : JSONObject.NULL);
        obj.put("tpep_dropoff_datetime", tpep_dropoff_datetime != null ? tpep_dropoff_datetime.format(formatter) : JSONObject.NULL);
        obj.put("passenger_count", passenger_count != null ? passenger_count : JSONObject.NULL);
        obj.put("trip_distance", trip_distance != null ? trip_distance : JSONObject.NULL);
        obj.put("RatecodeID", RatecodeID != null ? RatecodeID : JSONObject.NULL);
        obj.put("store_and_fwd_flag", store_and_fwd_flag != null ? store_and_fwd_flag : JSONObject.NULL);
        obj.put("PULocationID", PULocationID != null ? PULocationID : JSONObject.NULL);
        obj.put("DOLocationID", DOLocationID != null ? DOLocationID : JSONObject.NULL);
        obj.put("payment_type", payment_type != null ? payment_type : JSONObject.NULL);
        obj.put("fare_amount", fare_amount != null ? fare_amount : JSONObject.NULL);
        obj.put("extra", extra != null ? extra : JSONObject.NULL);
        obj.put("mta_tax", mta_tax != null ? mta_tax : JSONObject.NULL);
        obj.put("tip_amount", tip_amount != null ? tip_amount : JSONObject.NULL);
        obj.put("tolls_amount", tolls_amount != null ? tolls_amount : JSONObject.NULL);
        obj.put("improvement_surcharge", improvement_surcharge != null ? improvement_surcharge : JSONObject.NULL);
        obj.put("total_amount", total_amount != null ? total_amount : JSONObject.NULL);
        obj.put("congestion_surcharge", congestion_surcharge != null ? congestion_surcharge : JSONObject.NULL);
        obj.put("Airport_fee", Airport_fee != null ? Airport_fee : JSONObject.NULL);
        return obj.toString();
    }
}


/**
 * The PRODUCER task. Reads records from a Parquet file and puts them into a blocking queue.
 */
class ParquetReaderTask implements Runnable {
    private final String filePath;
    private final BlockingQueue<TripRecord> queue;
    private final AtomicBoolean isReadingFinished;
    private long delayMS;

    public ParquetReaderTask(String filePath, BlockingQueue<TripRecord> queue, AtomicBoolean isReadingFinished, long delayMS) {
        this.filePath = filePath;
        this.queue = queue;
        this.isReadingFinished = isReadingFinished;
        this.delayMS = delayMS;
    }

    @Override
    public void run() {
        System.out.println("Parquet Reader starting, " + delayMS + "ms delay per row" + "...");
        Path path = new Path(filePath);
        
        // TODO the parquet input file is not sorted! To "replay" it properly, will need to load
        // it in memory, sort it, and then emit it in sorted order.

        HadoopInputFile inputFile;
        try {
            inputFile = HadoopInputFile.fromPath(path, new Configuration());
        } catch (IOException e) {
            System.err.println("Error creating HadoopInputFile: " + e.getMessage());
            e.printStackTrace();
            return;
        }
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile)
                .withConf(new Configuration())
                .build()) {
            
            GenericRecord record;
            while ((record = reader.read()) != null) {
                queue.put(TripRecord.fromGenericRecord(record));
                Thread.sleep(delayMS); // Rate control: sleep after each record
            }
            System.out.println("Parquet Reader finished reading file.");

        } catch (IOException e) {
            System.err.println("Error reading Parquet file: " + e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Parquet Reader was interrupted.");
        } finally {
            isReadingFinished.set(true); // Signal that reading is complete
        }
    }
}

/**
 * The CONSUMER task. Takes records from the queue and sends them to Kafka.
 */
class KafkaProducerTask implements Runnable {
    private final BlockingQueue<TripRecord> queue;
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final AtomicBoolean isReadingFinished;

    public KafkaProducerTask(BlockingQueue<TripRecord> queue, String bootstrapServers, String topic, AtomicBoolean isReadingFinished) {
        this.queue = queue;
        this.topic = topic;
        this.isReadingFinished = isReadingFinished;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        System.out.println("Kafka Producer starting...");
        List<TripRecord> batch = new ArrayList<>();
        long recordsSent = 0;
        long lastReportTime = System.currentTimeMillis();

        try {
            // TODO this will only exit when reading is finished, but the queue may still have items
            // Loop as long as reading isn't finished
            while (!isReadingFinished.get()) {
                batch.clear();
                // Drain up to 1000 items from the queue. Don't block indefinitely.
                // TODO verify - does this really timeout if there arent any new items?
                int drained = queue.drainTo(batch, 1000); 

                if (drained > 0) {
                    
                    for (TripRecord record : batch) {
                        try {
                            String key = record.getKey();
                            String value = record.toJson();
                            producer.send(new ProducerRecord<>(topic, key, value), new Callback() {
                                @Override
                                public void onCompletion(org.apache.kafka.clients.producer.RecordMetadata metadata, Exception exception) {
                                    if (exception != null) {
                                        System.err.println("Error sending record to Kafka: " + exception.getMessage());
                                        exception.printStackTrace();
                                        // TODO shut everything down
                                    }
                                }
                            });
                        } catch (Exception e) {
                            System.err.println("Error invoking KafkaProducer.send() : " + e.getMessage());
                            e.printStackTrace();
                            return; // Exit on send error
                        }
                    }
                    producer.flush();
                    recordsSent += drained;
                }

                long now = System.currentTimeMillis();
                if (now - lastReportTime > 5000) { 
                    System.out.printf("Kafka Producer: Sent %,d records%n", recordsSent);
                    lastReportTime = now;
                }

                // If no records were drained, sleep briefly to prevent a busy-wait loop
                if (drained == 0) {
                    Thread.sleep(100);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Kafka Producer was interrupted.");
        } finally {
            producer.close();
            System.out.printf("Kafka Producer finished. Total records sent: %,d%n", recordsSent);
        }
    }
}
