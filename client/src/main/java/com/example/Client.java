package com.example;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import com.google.gson.JsonObject;
import java.io.*;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Client {
    private static String[] zookeeperIps = {"127.0.0.1"};
    private static int port = 8000;
    private static String masterIp = null;
    private static Socket clientSocket = null;
    private static ExecutorService executor = Executors.newSingleThreadExecutor();

    public static void main(String[] args) {
        try {
            // masterIp = findMaster();
            masterIp = "127.0.0.1";
            if (masterIp == null) {
                System.out.println("No master found");
                return;
            }
            clientSocket = openConnection(masterIp, port);
            System.out.println("Connected to server");

            while (true) {
                System.out.println("1. Push message");
                System.out.println("2. Pull message");
                System.out.println("3. Subscribe");
                System.out.println("4. Exit");

                Scanner scanner = new Scanner(System.in);
                int choice = scanner.nextInt();

                if (choice == 1) {
                    System.out.println("Enter key: ");
                    String key = scanner.next();
                    System.out.println("Enter value: ");
                    String value = scanner.next();
                    pushMessage(key, value);
                } else if (choice == 2) {
                    pullMessage();
                } else if (choice == 3) {
                    subscribe();
                } else if (choice == 4) {
                    break;
                } else {
                    System.out.println("Invalid choice");
                }
            }

            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String findMaster() throws IOException {
        OkHttpClient client = new OkHttpClient();
        for (String ip : zookeeperIps) {
            Request request = new Request.Builder()
                    .url("http://" + ip + "/zookeeper")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                if (response.isSuccessful()) {
                    return ip;
                }
            }
        }
        return null;
    }

    private static Socket openConnection(String nodeIp, int nodePort) throws IOException {
        return new Socket(nodeIp, nodePort);
    }

    private static void pushMessage(String key, String value) throws IOException {
        JsonObject message = new JsonObject();
        message.addProperty("type", "PUSH");
        message.addProperty("key", key);
        message.addProperty("value", value);
        message.addProperty("part_no", "0");

        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
        out.println(message.toString());

        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        String response = in.readLine();
        System.out.println("Received from server: " + response);
    }

    private static void pullMessage() throws IOException {
        JsonObject message = new JsonObject();
        message.addProperty("type", "PULL");

        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
        out.println(message.toString());

        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        String response = in.readLine();
        System.out.println("Received from server: " + response);
    }

    private static void subscribe() throws IOException {
        JsonObject message = new JsonObject();
        message.addProperty("type", "SUBSCRIBE");
        message.addProperty("broker_id", "0");

        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
        out.println(message.toString());

        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        String response = in.readLine();
        System.out.println("Received from server: " + response);

        executor.submit(Client::receiveMessage);
    }

    private static void receiveMessage() {
        try {
            clientSocket.setSoTimeout(3000);
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            while (true) {
                try {
                    String response = in.readLine();
                    System.out.println("Received from server: " + response);
                } catch (IOException e) {
                    continue;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}