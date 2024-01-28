import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.*;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import java.io.*;
import java.math.BigDecimal;
import java.net.InetSocketAddress;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

public class Main {
    private static CqlSession cqlSession;
    private static final String CASSANDRA_HOST = "localhost";
    private static final int CASSANDRA_PORT = 9042;
    private static final String KEYSPACE = "virtual_threads";
    private static ResultSet executeQuery(String query) {
        return cqlSession.execute(query);
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        // Set up the Cassandra session
        cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("localhost", 9042))
                .withLocalDatacenter("datacenter1")  // Set your actual local datacenter name here
                .build();

        // Set up the HTTP server
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        System.out.println("Basic Http VT Server started...");
        HttpContext context = server.createContext("/", new CrudHandler());

        // Start the HTTP server
        server.start();

        // Schedule performance monitoring task
//        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
//        scheduler.scheduleAtFixedRate(Main::monitorPerformance, 0, 5, TimeUnit.SECONDS);
    }

    private static void monitorPerformance() {
        // Track CPU and memory usage
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        double cpuUsage = osBean.getSystemLoadAverage();
        System.out.println("CPU Usage: " + cpuUsage + "%");

        // calc runtime and mem
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        long maxMemory = runtime.maxMemory();

        // print out stuff
//        writeToFile("cpuusage.csv", cpuUsage + "");
//        writeToFile("ramusage.csv", usedMemory / (1024 * 1024) + "," + maxMemory / (1024 * 1024));
    }

    static class CrudHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Runnable run = () -> {
                String requestMethod = exchange.getRequestMethod();
                try {
                    if (requestMethod.equalsIgnoreCase("GET")) {
                        handleGetRequest(exchange);
                    } else if (requestMethod.equalsIgnoreCase("POST")) {
                        handlePostRequest(exchange);
                    } else if (requestMethod.equalsIgnoreCase("PUT")) {
                        handlePutRequest(exchange);
                    } else if (requestMethod.equalsIgnoreCase("DELETE")) {
                        handleDeleteRequest(exchange);
                    } else {
                        sendResponse(exchange, 400, "Bad Request");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            };

            // Virtual Threads
            Thread.startVirtualThread(run);

            // Normal Threads
//            Thread thread = new Thread(run);
//            thread.start();
        }

        private void handleGetRequest(HttpExchange exchange) throws IOException {
            String query = "SELECT * FROM virtual_threads.temps";
            ResultSet resultSet = executeQuery(query);

            StringBuilder response = new StringBuilder();
            resultSet.forEach(row -> {
                int year = row.getInt("year");
                System.out.println(year);
                BigDecimal temp = row.getBigDecimal("temp");
                response.append(String.format("Year: %d, Temp: %f%n", year, temp));
            });

            sendResponse(exchange, 200, response.toString());
        }

        private void handlePostRequest(HttpExchange exchange) throws IOException {
            InputStream requestBody = exchange.getRequestBody();
            InputStreamReader isr = new InputStreamReader(requestBody);
            BufferedReader br = new BufferedReader(isr);

            // Assuming JSON data in the request body
            JsonObject json = JsonParser.parseReader(br).getAsJsonObject();
            int year = json.getAsJsonPrimitive("year").getAsInt();
            double temp = json.getAsJsonPrimitive("temp").getAsDouble();

            String keyspace = "virtual_threads";
            String query = "INSERT INTO temps (year, temp) VALUES (?, ?)";
            executeUpdate(keyspace, query, year, temp);

            sendResponse(exchange, 200, "Data inserted successfully");
        }

        private void handlePutRequest(HttpExchange exchange) throws IOException {
            InputStream requestBody = exchange.getRequestBody();
            InputStreamReader isr = new InputStreamReader(requestBody);
            BufferedReader br = new BufferedReader(isr);

            // Assuming JSON data in the request body
            JsonObject json = JsonParser.parseReader(br).getAsJsonObject();
            int year = json.getAsJsonPrimitive("year").getAsInt();
            double temp = json.getAsJsonPrimitive("temp").getAsDouble();

            String query = "UPDATE temps SET temp = ? WHERE year = ?";
            String keyspace = "virtual_threads";

            executeUpdate(keyspace,query, temp, year);

            sendResponse(exchange, 200, "Data updated successfully");
        }

        private void handleDeleteRequest(HttpExchange exchange) throws IOException {
            InputStream requestBody = exchange.getRequestBody();
            InputStreamReader isr = new InputStreamReader(requestBody);
            BufferedReader br = new BufferedReader(isr);

            // Assuming JSON data in the request body
            JsonObject json = JsonParser.parseReader(br).getAsJsonObject();
            int year = json.getAsJsonPrimitive("year").getAsInt();

            String query = "DELETE FROM temps WHERE year = ?";
            String keyspace = "virtual_threads";
            executeUpdate(keyspace,query, year);

            sendResponse(exchange, 200, "Data deleted successfully");
        }



        private void executeUpdate(String keyspace, String query, Object... values) {
            // Set the keyspace for the session
            cqlSession.execute("USE " + keyspace);

            // Create the SimpleStatement with placeholders and pass the values
            SimpleStatement statement = SimpleStatement
                    .newInstance(query, values)
                    .setKeyspace(keyspace);

            // Execute the query
            cqlSession.execute(statement);
        }






        private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
            Headers headers = exchange.getResponseHeaders();
            headers.set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(statusCode, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
}


