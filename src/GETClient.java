import java.io.*;
import java.net.*;
import com.google.gson.*;

public class GETClient {
    private static LamportClock clock = new LamportClock();

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: java GETClient <host:port>");
            return;
        }
        String[] parts = args[0].split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        Gson gson = new Gson();

        clock.tick();

        try (Socket socket = new Socket(host, port);
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.write("GET /weather.json HTTP/1.1\r\n");
            out.write("Host: " + host + "\r\n");
            out.write("Lamport-Clock: " + clock.getTime() + "\r\n\r\n");
            out.flush();

            String line = in.readLine();
            if (line == null) return;
            System.out.println(line); // Status line
            int n = 0;
            while ((line = in.readLine()) != null && !line.trim().isEmpty()) {
                if (line.startsWith("Lamport-Clock:")) {
                    int serverClock = Integer.parseInt(line.split(":")[1].trim());
                    clock.update(serverClock);
                }
                n++;
            }
            // Print JSON body, each attribute-value on its own line
            while ((line = in.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                JsonObject obj = gson.fromJson(line, JsonObject.class);
                for (String key : obj.keySet()) {
                    System.out.println(key + ": " + obj.get(key).getAsString());
                }
                System.out.println();
            }
        }
    }
}
