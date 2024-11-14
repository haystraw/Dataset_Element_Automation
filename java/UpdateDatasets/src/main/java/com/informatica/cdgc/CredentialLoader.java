package com.informatica.cdgc;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;
import javafx.stage.Stage;
import org.json.JSONObject;
import java.util.Base64;

import javax.swing.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.stream.Collectors;

public class CredentialLoader extends Application {

    private static String defaultPod = "";
    private static String defaultUser = "";
    private static String defaultPwd = "";

    @Override
    public void start(Stage primaryStage) {
        loadCredentialsFromFile();
        showHtmlForm(primaryStage);
    }

    public String encodeImageToBase64(String image_path) {
        try {
            byte[] imageBytes = Files.readAllBytes(Path.of(image_path));
            return Base64.getEncoder().encodeToString(imageBytes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // Loads credentials from the file if available
    private boolean loadCredentialsFromFile() {
        Path credentialsPath = Paths.get(System.getProperty("user.home"), ".informatica_cdgc", "credentials.json");

        if (Files.exists(credentialsPath)) {
            try {
                String content = Files.readString(credentialsPath);
                JSONObject credentials = new JSONObject(content);
                defaultPod = credentials.optString("default_pod", "");
                defaultUser = credentials.optString("default_user", "");
                defaultPwd = credentials.optString("default_pwd", "");
                return defaultPod != null && defaultUser != null && defaultPwd != null;
            } catch (IOException e) {

            }
        }
        return false;
    }

    // Display HTML form in a WebView for missing credentials
    private void showHtmlForm(Stage primaryStage) {

        System.out.println("DEBUG defaults: " + defaultPod + " | " + defaultUser + " | " + defaultPwd);

        WebView webView = new WebView();
        WebEngine webEngine = webView.getEngine();

        // Load HTML file as a string
        String htmlContent;
        try (InputStream inputStream = getClass().getResourceAsStream("/credentials_form.html");
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

            htmlContent = reader.lines().collect(Collectors.joining("\n"));

            // Replace placeholder with actual logo path

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        htmlContent = htmlContent.replace("{{defaultPod}}", defaultPod);
        htmlContent = htmlContent.replace("{{defaultUser}}", defaultUser);
        htmlContent = htmlContent.replace("{{defaultPwd}}", defaultPwd);
        // System.out.println("DEBUGSCOTT HTML: Source Code...");
        // System.out.println(htmlContent);
        // Load modified HTML content into WebEngine

        String logoImageUrl = getClass().getResource("/informatica_logo.jpg").toExternalForm();
        htmlContent = htmlContent.replace("{{logoImageUrl}}", logoImageUrl);

        webEngine.loadContent(htmlContent);

        // Handle alert (form submission) to capture data
        webEngine.setOnAlert(event -> {
            JSONObject credentials = new JSONObject(event.getData());
            defaultPod = credentials.getString("pod");
            defaultUser = credentials.getString("username");
            defaultPwd = credentials.getString("password");
            Platform.exit(); // Close the application after submission
        });

        Image icon_16 = new Image(getClass().getResource("/favicon.ico.16.png").toExternalForm());
        Image icon_32 = new Image(getClass().getResource("/favicon.ico.32.png").toExternalForm());
        Image icon_64 = new Image(getClass().getResource("/favicon.ico.64.png").toExternalForm());

        primaryStage.getIcons().addAll(icon_16, icon_32, icon_64);

        primaryStage.setScene(new Scene(webView, 500, 500));
        primaryStage.setTitle("Enter Credentials");
        primaryStage.show();
    }

    // Getter methods to access the credentials
    public static String getDefaultPod() {
        return defaultPod;
    }

    public static String getDefaultUser() {
        return defaultUser;
    }

    public static String getDefaultPwd() {
        return defaultPwd;
    }

    public static void loadCredentials() {
        // Launch JavaFX application to load or prompt for credentials
        Application.launch(CredentialLoader.class);
    }

    public static void main(String[] args) {
        loadCredentials();
        // Now you can use getDefaultPod(), getDefaultUser(), and getDefaultPwd()
        System.out.println("Pod: " + getDefaultPod());
        System.out.println("User: " + getDefaultUser());
        System.out.println("Password: " + (getDefaultPwd() != null ? "******" : "Not Set"));
    }
}
