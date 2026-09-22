package com.nethealth.monitor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NetworkMonitorApp {

    // Output log file name
    private static final String LOG_FILE = "nethealth.log";

    // Data model representing a target network endpoint
    static class ServiceNode {
        String nodeName;
        String endpointUrl;

        public ServiceNode(String nodeName, String endpointUrl) {
            this.nodeName = nodeName;
            this.endpointUrl = endpointUrl;
        }
    }

    public static void main(String[] args) {
        // 1. Initialize target monitoring nodes
        List<ServiceNode> nodes = new ArrayList<>();
        nodes.add(new ServiceNode("Primary Gateway Node", "https://www.google.com"));
        nodes.add(new ServiceNode("Auth Service Cluster", "https://www.github.com"));
        nodes.add(new ServiceNode("Cisco Web Portal", "https://www.cisco.com"));
        nodes.add(new ServiceNode("Simulated Fault Node", "https://httpbin.org/status/404"));

        // 2. Thread-safe map to record failure metrics concurrently
        Map<String, Integer> failureStats = new ConcurrentHashMap<>();

        System.out.println("=================================================");
        System.out.println("  NetHealth: Concurrent Network Service Monitor  ");
        System.out.println("=================================================\n");

        long startTime = System.currentTimeMillis();

        // 3. Create a thread pool with 4 fixed worker threads
        ExecutorService threadPool = Executors.newFixedThreadPool(4);

        System.out.println(">>> Executing concurrent health probes...");

        // 4. Submit probing tasks to the thread pool
        for (ServiceNode node : nodes) {
            threadPool.submit(() -> checkHealth(node, failureStats));
        }

        // 5. Shutdown thread pool and wait for task completion
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Thread pool execution interrupted: " + e.getMessage());
        }

        long totalDuration = System.currentTimeMillis() - startTime;

        // 6. Summary Report
        System.out.println("\n================ Probe Summary ================");
        System.out.println("Execution Completed in: " + totalDuration + " ms");
        System.out.println("Logs saved to local file: " + LOG_FILE);
        System.out.println("Absolute Log Path: " + new File(LOG_FILE).getAbsolutePath() + "\n");

        for (String name : failureStats.keySet()) {
            System.out.println("Node [" + name + "] Failures Recorded: " + failureStats.get(name));
        }
    }

    // Core probing logic with thread safety and latency measurement
    public static void checkHealth(ServiceNode node, Map<String, Integer> failureStats) {
        long startTime = System.currentTimeMillis();
        String threadName = Thread.currentThread().getName();
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        String logMessage;

        try {
            URL url = new URL(node.endpointUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000); // 5-second timeout limit
            connection.setReadTimeout(5000);

            int statusCode = connection.getResponseCode();
            long latency = System.currentTimeMillis() - startTime;

            if (statusCode >= 200 && statusCode < 300) {
                logMessage = String.format("[%s] [%s] [SUCCESS] %s | Status: %d | Latency: %dms", 
                                           timestamp, threadName, node.nodeName, statusCode, latency);
            } else {
                logMessage = String.format("[%s] [%s] [WARNING] %s | Status: %d | Latency: %dms", 
                                           timestamp, threadName, node.nodeName, statusCode, latency);
                failureStats.merge(node.nodeName, 1, Integer::sum);
            }
            connection.disconnect();
        } catch (Exception e) {
            long latency = System.currentTimeMillis() - startTime;
            logMessage = String.format("[%s] [%s] [ERROR] %s | Connection Failed/Timeout | Latency: %dms", 
                                       timestamp, threadName, node.nodeName, latency);
            failureStats.merge(node.nodeName, 1, Integer::sum);
        }

        // Output to console
        System.out.println(logMessage);

        // Append log to local file (Thread-safe File I/O)
        writeLogToFile(logMessage);
    }

    // Synchronized file writer to prevent concurrent writing data races
    private static synchronized void writeLogToFile(String logMessage) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE, true))) {
            writer.write(logMessage);
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Failed to write log to file: " + e.getMessage());
        }
    }
}