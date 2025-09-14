import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class AggregationServer {
    private static final int EXPIRY_MS = 30000; // 30 seconds
    private static final String DATA_STORE = "server_data.json";
    private static final Map<String, WeatherRecord> data = new ConcurrentHashMap<>();
    private static final LamportClock clock = new LamportClock();
    private static final Gson gson = new Gson();

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: java AggregationServer <port>");
            return;
        }
        int port = Integer.parseInt(args[0]);
        loadFromDisk();
        ScheduledExecutorService expiryService = Executors.newSingleThreadScheduledExecutor();
        expiryService.scheduleAtFixedRate(() -> removeExpired(), 2, 2, TimeUnit.SECONDS);

        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("AggregationServer running on port " + port);

        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(() -> handleConnection(socket)).start();
        }
    }

    private static void removeExpired() {
        long now = System.currentTimeMillis();
        data.entrySet().removeIf(entry -> now - entry.getValue().timestamp > EXPIRY_MS);
        saveToDisk();
    }

    private static void handleConnection(Socket socket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {

            String line = in.readLine();
            if (line == null) return;
            String method = line.startsWith("PUT") ? "PUT" : line.startsWith("GET") ? "GET" : "";

            if (method.equals("PUT")) handlePut(line, in, out);
            else if (method.equals("GET")) handleGet(out, in);
            else {
                out.write("HTTP/1.1 400 Bad Request\r\n\r\n");
                out.flush();
            }
        } catch (Exception e) {
            //e.printStackTrace();
        } finally {
            try { socket.close(); } catch (Exception ignore) {}
        }
    }

    private static void handlePut(String requestLine, BufferedReader in, BufferedWriter out) throws IOException {
        int lamportReceived = 0;
        int contentLength = 0;
        String line;
        // HTTP Header parsing
        while (!(line = in.readLine()).isEmpty()) {
            if (line.startsWith("Lamport-Clock:")) lamportReceived = Integer.parseInt(line.split(":")[1].trim());
            else if (line.startsWith("Content-Length:")) contentLength = Integer.parseInt(line.split(":")[1].trim());
        }

        char[] body = new char[contentLength];
        in.read(body);
        String json = new String(body);

        // Validate and parse JSON
        JsonObject obj;
        try {
            obj = gson.fromJson(json, JsonObject.class);
            if (!obj.has("id")) throw new Exception();
        } catch (Exception e) {
            out.write("HTTP/1.1 500 Internal Server Error\r\n\r\nInvalid JSON or missing 'id'.");
            out.flush();
            return;
        }
        String id = obj.get("id").getAsString();

        clock.update(lamportReceived);
        boolean isFirst = !data.containsKey(id);
        data.put(id, new WeatherRecord(obj, clock.getTime()));
        saveToDisk();

        out.write(isFirst ? "HTTP/1.1 201 Created\r\n" : "HTTP/1.1 200 OK\r\n");
        out.write("Lamport-Clock: " + clock.getTime() + "\r\n");
        out.write("\r\n");
        out.flush();
    }

    private static void handleGet(BufferedWriter out, BufferedReader in) throws IOException {
        clock.tick();
        removeExpired();
        if (data.isEmpty()) {
            out.write("HTTP/1.1 204 No Content\r\n\r\n");
        } else {
            out.write("HTTP/1.1 200 OK\r\n");
            out.write("Lamport-Clock: " + clock.getTime() + "\r\n");
            out.write("Content-Type: application/json\r\n\r\n");
            for (WeatherRecord record : data.values()) {
                out.write(gson.toJson(record.obj));
                out.write("\r\n"); // Send each JSON object on a new line if multiple exist
            }
        }
        out.flush();
    }

    private static void saveToDisk() {
        try (FileWriter fw = new FileWriter(DATA_STORE)) {
            for (WeatherRecord record : data.values()) {
                fw.write(gson.toJson(record.obj));
                fw.write("\n");
            }
        } catch (IOException e) {
            // ignore for brevity
        }
    }
    private static void loadFromDisk() {
        try (BufferedReader br = new BufferedReader(new FileReader(DATA_STORE))) {
            String line;
            while ((line = br.readLine()) != null) {
                JsonObject obj = gson.fromJson(line, JsonObject.class);
                String id = obj.get("id").getAsString();
                data.put(id, new WeatherRecord(obj, 0));
            }
        } catch (IOException ignore) {}
    }
}
