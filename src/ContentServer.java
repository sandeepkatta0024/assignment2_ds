import java.io.*;
import java.net.*;
import java.util.*;
import com.google.gson.*;

public class ContentServer {
    private static LamportClock clock = new LamportClock();

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: java ContentServer <host:port> <datafile>");
            return;
        }
        String[] parts = args[0].split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        String filePath = args[1];
        Gson gson = new Gson();

        // Parse text file into JSON
        Map<String, String> map = new LinkedHashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                int idx = line.indexOf(':');
                if (idx < 0) continue;
                String key = line.substring(0, idx).trim();
                String value = line.substring(idx + 1).trim();
                map.put(key, value);
            }
        }
        if (!map.containsKey("id")) {
            System.out.println("Data file must contain 'id' field.");
            return;
        }
        String json = gson.toJson(map);

        // PUT to server
        while (true) {
            try (Socket socket = new Socket(host, port);
                 BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                clock.tick();

                out.write("PUT /weather.json HTTP/1.1\r\n");
                out.write("Host: " + host + "\r\n");
                out.write("Content-Type: application/json\r\n");
                out.write("Lamport-Clock: " + clock.getTime() + "\r\n");
                out.write("Content-Length: " + json.length() + "\r\n");
                out.write("\r\n");
                out.write(json);
                out.flush();

                String response = in.readLine();
                System.out.println("Server: " + response);
                while ((response = in.readLine()) != null && !response.isEmpty()) {
                    if (response.startsWith("Lamport-Clock:")) {
                        int servClock = Integer.parseInt(response.split(":")[1].trim());
                        clock.update(servClock);
                    }
                }
                break; // Once succeeded, exit
            } catch (IOException e) {
                System.out.println("Retrying connection in 2s...");
                Thread.sleep(2000);
            }
        }
    }
}
