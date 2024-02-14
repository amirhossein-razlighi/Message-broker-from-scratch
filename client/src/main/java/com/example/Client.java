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
import java.util.concurrent.Future;

public class Client {
    private static String[] zookeeperIps = {"127.0.0.1"};
    private static int port = 8000;
    private static String masterIp = null;
    private static Socket clientSocket = null;
    private static Socket clientSubscribeSocket = null;
    private static ExecutorService executor = Executors.newSingleThreadExecutor();

    public static void main(String[] args) {
        try {
            // masterIp = findMaster();
            if (masterIp == null) {
                System.out.println("No master found");
                // return;
                masterIp = zookeeperIps[0];
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
        if (response.startsWith("Brokers")) {
            System.out.println(response);
            return;
        }
        String[] parts = response.split(",");
        String hostB = parts[0];
        int portB = Integer.parseInt(parts[1]);
        int partNo = Integer.parseInt(parts[2]);
        message.addProperty("part_no", partNo);

        Socket newClientSocket = new Socket(hostB, portB);
        PrintWriter newOut = new PrintWriter(newClientSocket.getOutputStream(), true);
        newOut.println(message.toString());

        BufferedReader newIn = new BufferedReader(new InputStreamReader(newClientSocket.getInputStream()));
        String newResponse = newIn.readLine();
        System.out.println("Received from server: " + newResponse);

        newClientSocket.close();
    }

    private static void subscribe() throws IOException {
        JsonObject message = new JsonObject();
        message.addProperty("type", "SUBSCRIBE");

        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
        out.println(message.toString());

        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        String response = in.readLine();
        if (response.startsWith("There is not")) {
            System.out.println(response);
            return;
        }
        String[] parts = response.split(",");
        String hostB = parts[0];
        int portB = Integer.parseInt(parts[1]);
        int partNo = Integer.parseInt(parts[2]);

        clientSubscribeSocket = new Socket(hostB, portB);
        PrintWriter subscribeOut = new PrintWriter(clientSubscribeSocket.getOutputStream(), true);
        JsonObject subscribeMessage = new JsonObject();
        subscribeMessage.addProperty("type", "SUBSCRIBE");
        subscribeMessage.addProperty("broker_id", "0");
        subscribeMessage.addProperty("part_no", partNo);
        subscribeOut.println(subscribeMessage.toString());

        BufferedReader subscribeIn = new BufferedReader(new InputStreamReader(clientSubscribeSocket.getInputStream()));
        String subscribeResponse = subscribeIn.readLine();
        System.out.println("Received from server: " + subscribeResponse);

        executor.submit(() -> receiveMessage());
    }

    private static void receiveMessage() {
        try {
            clientSubscribeSocket.setSoTimeout(6000);
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSubscribeSocket.getInputStream()));
            while (true) {
                try {
                    String response = in.readLine();
                    if (!response.startsWith("No message")) {
                        System.out.println("Received from server: " + response);
                    }
                } catch (IOException e) {
                    continue;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
