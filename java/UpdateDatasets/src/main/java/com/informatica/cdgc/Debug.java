package com.informatica.cdgc;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import org.json.*;

public class Debug {

        private static String defaultPod;
        private static String defaultUser;
        private static String defaultPwd;
        private static String extractsFolder;
        private static String defaultConfigFile;

        private static String sessionID;
        private static String orgID;
        private static String iicsUrl;
        private static String cdgcUrl;
        private static Map<String, String> headersBearer = new HashMap<>();

        // Method to login and set session details
        public static void login(String pod, String user, String pwd) throws IOException, InterruptedException {
                iicsUrl = "https://" + pod + ".informaticacloud.com";
                cdgcUrl = "https://cdgc-api." + pod + ".informaticacloud.com";

                HttpClient client = HttpClient.newHttpClient();
                String loginUrl = iicsUrl + "/saas/public/core/v3/login";
                JSONObject loginData = new JSONObject()
                                .put("username", user)
                                .put("password", pwd);

                HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(loginUrl))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(loginData.toString()))
                                .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                JSONObject jsonResponse = new JSONObject(response.body());

                sessionID = jsonResponse.getJSONObject("userInfo").getString("sessionId");
                orgID = jsonResponse.getJSONObject("userInfo").getString("orgId");

                String tokenUrl = iicsUrl
                                + "/identity-service/api/v1/jwt/Token?client_id=cdlg_app&nonce=g3t69BWB49BHHNn&access_code=";

                HttpRequest token_request = HttpRequest.newBuilder()
                                .uri(URI.create(tokenUrl))
                                .header("Content-Type", sessionID)
                                .header("INFA-SESSION-ID", sessionID)
                                .header("IDS-SESSION-ID", sessionID)
                                .header("icSessionId", sessionID)
                                .POST(HttpRequest.BodyPublishers.ofString(loginData.toString()))
                                .build();

                HttpResponse<String> token_response = client.send(token_request, HttpResponse.BodyHandlers.ofString());
                JSONObject token_jsonResponse = new JSONObject(token_response.body());
                if (!token_jsonResponse.has("jwt_token")) {
                        throw new JSONException("Error: 'jwt_token' not found in response: " + response.body());
                }
                String this_token = token_jsonResponse.getString("jwt_token");
                headersBearer.put("Authorization", "Bearer " + this_token);
        }

        // Method to process search and save results
        public static void processSearch(String searchName, Map<String, Object> tokens)
                        throws IOException, InterruptedException {
                login(defaultPod, defaultUser, defaultPwd);
                // Add your search criteria here based on tokens
                // For simplicity, I'm assuming search parameters are created here as JSON

                JSONObject searchPayload = new JSONObject(); // Build this from tokens or use predefined searches
                String saveFilename = searchName + "_results.json"; // Simulated filename

                HttpClient client = HttpClient.newHttpClient();
                HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(cdgcUrl + "/ccgf-searchv2/api/v1/search"))
                                .header("Content-Type", "application/json")
                                .header("X-INFA-SEARCH-LANGUAGE", "elasticsearch")
                                .header("Authorization", headersBearer.get("Authorization"))
                                .POST(HttpRequest.BodyPublishers.ofString(searchPayload.toString()))
                                .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                // Save the response to a file
                Path path = Paths.get(extractsFolder, saveFilename);
                Files.createDirectories(path.getParent());
                Files.writeString(path, response.body(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        }

        public static void main(String[] args) throws IOException, InterruptedException {
                // Setting up configurations
                String configFile = args.length > 0 ? args[0] : defaultConfigFile;

                // Perform login
                CredentialLoader.loadCredentials();

                String defaultPod = CredentialLoader.getDefaultPod();
                String defaultUser = CredentialLoader.getDefaultUser();
                String defaultPwd = CredentialLoader.getDefaultPwd();
                System.out.printf("Fetched variables:  defaultPod: %s | defaultUser: %s | defaultPwd: %s %n",
                                defaultPod,
                                defaultUser, defaultPwd);

                // login(defaultPod, defaultUser, defaultPwd);

                // Process search as an example
                // Map<String, Object> tokens = new HashMap<>();
                // processSearch("All Resources", tokens);

                // Placeholder for further actions
                System.out.println("Process completed");
        }
}
