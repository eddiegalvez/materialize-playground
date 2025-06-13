package com.example;

import io.javalin.Javalin;
import io.javalin.websocket.WsContext;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class DashboardApp {

    // --- DATABASE CONFIGURATION (for Materialize) ---
    private static final String DB_URL = "jdbc:postgresql://localhost:6875/materialize";
    private static final String DB_USER = "materialize";
    private static final String DB_PASSWORD = "materialize";

    // Manages active WebSocket connections
    private static final Map<String, WsContext> userUsernameMap = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        Javalin app = Javalin.create(config -> {
            // Serve static files from the 'src/main/resources/public' directory
            config.staticFiles.add("/public");
        }).start(7070);

        // Setup the WebSocket endpoint
        app.ws("/dashboard-data", ws -> {
            ws.onConnect(ctx -> {
                System.out.println("Client connected: " + ctx.sessionId());
                userUsernameMap.put(ctx.sessionId(), ctx);
            });
            ws.onClose(ctx -> {
                System.out.println("Client disconnected: " + ctx.sessionId());
                userUsernameMap.remove(ctx.sessionId());
            });
            ws.onError(ctx -> {
                System.err.println("WebSocket error on session " + ctx.sessionId());
                userUsernameMap.remove(ctx.sessionId());
            });
        });

        // Start background threads to subscribe and process data
        // TODO look for a better pattern than one-thread-per-query
        startBusiestLocationThread();
        startTopTippingTripsThread();
        startPopularTripsThread();

        System.out.println("Dashboard server started on http://localhost:7070");
    }

    private static final Properties DB_PROPS = new Properties();
    static {
        DB_PROPS.setProperty("user", DB_USER);
        DB_PROPS.setProperty("password", DB_PASSWORD);
        DB_PROPS.setProperty("ssl","false");
    }

    private static volatile boolean running = true;

    private static void startBusiestLocationThread() {
        Thread thread = new Thread(() -> {
            while (running) {
                try (Connection conn = DriverManager.getConnection(DB_URL, DB_PROPS)) {

                    while (running) {

                         // This is a "run a query periodically" example, see below for a "subscribe" example
                        String sql = "SELECT \"PULocationID\", trip_count FROM busiest_pickup_locations ORDER BY trip_count DESC;";
                        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                            JSONArray labels = new JSONArray();
                            JSONArray data = new JSONArray();

                            while (rs.next()) {
                                labels.put("Location " + rs.getLong("PULocationID"));
                                data.put(rs.getLong("trip_count"));
                            }

                            JSONObject json = new JSONObject();
                            json.put("labels", labels);
                            json.put("data", data);

                            if (!userUsernameMap.isEmpty()) {
                                for (WsContext ctx : userUsernameMap.values()) {
                                    ctx.send(json.toString());
                                }
                            }
                        }

                        Thread.sleep(1000); // Polling interval


                        // WISH an api wrapper with concepts like 'register'
                        // to register multiple subscriptions and receive events / callbacks
                        // for any/all of them in one spot
                        
    //                     // busiest pickup locations subscription query
    //                     Statement stmt = conn.createStatement();
    //                     stmt.execute("BEGIN");
    //                     stmt.execute("DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM busiest_pickup_locations)");
    //                     while (running) {
    //                         ResultSet rs = stmt.executeQuery("FETCH ALL c");

    //                         //
    //                         // example full row (since we don't update anything, every row is a new row)
    //                         //
    //                         //mz_timestamp: 1749764084000, mz_diff: 1,
    //                         // VendorID: 1, tpep_pickup_datetime: 2024-06-01 00:12:24, tpep_dropoff_datetime: 2024-06-01 00:28:20, passenger_count: 1,
    //                         // trip_distance: 6, RatecodeID: 1, store_and_fwd_flag: N, PULocationID: 33, DOLocationID: 79, payment_type: 1,
    //                         // fare_amount: 25.4, extra: 3.5,  mta_tax: 0.5, tip_amount: 6.1, tolls_amount: 0, improvement_surcharge: 1,
    //                         // total_amount: 36.5, congestion_surcharge: 2.5, Airport_fee: 0
    //                         //
    //                         ResultSetMetaData metaData = rs.getMetaData();
    // int columnCount = metaData.getColumnCount();
    // if (rs.next()) {
    //     StringBuilder row = new StringBuilder();
    //     for (int i = 1; i <= columnCount; i++) {
    //         row.append(metaData.getColumnName(i)).append(": ").append(rs.getString(i));
    //         if (i < columnCount) row.append(", ");
    //     }
    //     System.out.println(row);
    // }

                         
    //                     }

            
                       
                    }
                } catch (SQLException e) {
                    System.err.println("Materialize SQLException: " + e.getMessage());
                    System.exit(1);
                    try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                } catch (Exception e) {
                    System.err.println("An unexpected error occurred in data subscriber: " + e.getMessage());
                    try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

     private static void startPopularTripsThread() {
        Thread thread = new Thread(() -> {
            while (running) {

                try (Connection conn = DriverManager.getConnection(DB_URL, DB_PROPS)) {

                    while (running) {

                        String sql = "SELECT * FROM popular_trips ORDER BY number_of_trips DESC;";
                        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                            JSONArray trips = new JSONArray();
                            
                            while (rs.next()) {
                                JSONObject trip = new JSONObject();

                                 trip.put("pickup_location", rs.getString("pickup_location"));
                                 trip.put("dropoff_location", rs.getString("dropoff_location"));
                                 trip.put("number_of_trips", rs.getInt("number_of_trips"));
                                 
                                 trips.put(trip); 
                            }

                            JSONObject json = new JSONObject();
                            json.put("popularTrips", trips);
                            
                            if (!userUsernameMap.isEmpty()) {
                                for (WsContext ctx : userUsernameMap.values()) {
                                    ctx.send(json.toString());
                                }
                            }
                        }

                        Thread.sleep(1000); // Polling interval            
                       
                    }
                } catch (SQLException e) {
                    System.err.println("Materialize SQLException: " + e.getMessage());
                    System.exit(1);
                    try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                } catch (Exception e) {
                    System.err.println("An unexpected error occurred in data subscriber: " + e.getMessage());
                    try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    private static void startTopTippingTripsThread() {
        Thread thread = new Thread(() -> {
            while (running) {

                try (Connection conn = DriverManager.getConnection(DB_URL, DB_PROPS)) {

                    while (running) {

                        String sql = "SELECT * FROM top_tipped_trips ORDER BY tip_amount DESC;";
                        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                            JSONArray trips = new JSONArray();
                            
                            while (rs.next()) {
                                JSONObject trip = new JSONObject();

                                 trip.put("tpep_pickup_datetime", rs.getString("tpep_pickup_datetime"));
                                 trip.put("tpep_dropoff_datetime", rs.getString("tpep_dropoff_datetime"));
                                 trip.put("passenger_count", rs.getInt("passenger_count"));
                                 trip.put("trip_distance", rs.getDouble("trip_distance"));
                                 trip.put("fare_amount", rs.getDouble("fare_amount"));
                                 trip.put("tip_amount", rs.getDouble("tip_amount"));
                                 trip.put("total_amount", rs.getDouble("total_amount"));
                                 
                                 trips.put(trip); 
                            }

                            JSONObject json = new JSONObject();
                            json.put("mostTippedTrips", trips);
                            
                            if (!userUsernameMap.isEmpty()) {
                                for (WsContext ctx : userUsernameMap.values()) {
                                    ctx.send(json.toString());
                                }
                            }
                        }

                        Thread.sleep(1000); // Polling interval            
                       
                    }
                } catch (SQLException e) {
                    System.err.println("Materialize SQLException: " + e.getMessage());
                    System.exit(1);
                    try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                } catch (Exception e) {
                    System.err.println("An unexpected error occurred in data subscriber: " + e.getMessage());
                    try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }
   
}
