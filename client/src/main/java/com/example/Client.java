package com.example;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.google.gson.Gson;

public class Client {

    private static final Logger LOGGER = Logger.getLogger(Client.class.getName());
    private static final String[] ZOOKEEPER_IPS = {"127.0.0.1"};
    private static final int PORT = 8000;
    private static String masterIp = null;
    private static Socket clientSocket = null;
    private static PrintWriter out = null;
    private static BufferedReader in = null;
   
    public static void main(String[] args) throws IOException {
        LOGGER.log(Level.INFO, "Client started");
        // String hostName = System.getenv("BROKER");
        String hostName = "127.0.0.1";
        clientSocket = openConnection(hostName, PORT);

        // Example usage:
        // pushMessage("Hello from JAVA", "world");

        
        // Example usage with async pull and subscribe:
        new Thread(() -> {
            try {
                subscribe();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        try {
            Thread.sleep(6000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < 10; i++) {
            pushMessage(String.valueOf(i), "world " + i);
            pushMessage(String.valueOf(i), "world " + (i + 1));
            pushMessage(String.valueOf(i), "world " + (i + 2));
        }

        try {
            Thread.sleep(12000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < 10; i++) {
            try {
                pullMessage();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        clientSocket.close();
        
    }

    private static String findMaster() throws IOException {
        for (String ip : ZOOKEEPER_IPS) {
            URL url = new URL("http://" + ip + "/zookeeper");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            int responseCode = connection.getResponseCode();
            if (responseCode == 200) {
                return ip;
            }
        }
        return null;
    }
    
    private static Socket openConnection(String nodeIp, int nodePort) {
        try {
            String hostIp = java.net.InetAddress.getByName(nodeIp).getHostAddress();
            int portCnn = nodePort;
            Socket newSocket = new Socket(hostIp, portCnn);
            out = new PrintWriter(newSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(newSocket.getInputStream()));
            System.out.println("Connected to server");
            return newSocket;
        } catch (UnknownHostException e) {
            LOGGER.log(Level.SEVERE, "Unknown host: " + nodeIp);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "IOException occurred while opening connection", e);
        }
        return null;
    }

    private static void pushMessage(String key, String value) {
        if (clientSocket == null || out == null || in == null) {
            // Handle error
            return;
        }
        try {
            out.println("{\"type\":\"PUSH\",\"key\":\"" + key + "\",\"value\":\"" + value + "\",\"part_no\":\"0\"}");
            String response = in.readLine();
            System.out.println("Received from server: " + response);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "IOException occurred while pushing message", e);
        }
    }

    private static void pullMessage() throws IOException {
        if (clientSocket == null) {
            // Handle error
            return;
        }
        OutputStream outputStream = clientSocket.getOutputStream();
        PrintWriter out = new PrintWriter(outputStream, true);
        out.println("{\"type\":\"PULL\"}");
        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        String response = in.readLine();
        System.out.println("Received from server: " + response);
    }

   private static void subscribe() throws IOException {
        if (clientSocket == null || out == null || in == null) {
            // Handle error
            return;
        }
        Map<String, String> message = new HashMap<>();
        message.put("type", "SUBSCRIBE");
        message.put("broker_id", "0");
        Gson gson = new Gson();
        String jsonMessage = gson.toJson(message);
        out.println(jsonMessage);
        String response = in.readLine();
        System.out.println("Received from server: " + response);
    }
}
