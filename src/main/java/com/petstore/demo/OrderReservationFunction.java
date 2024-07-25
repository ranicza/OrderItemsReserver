package com.petstore.demo;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

/**
 * Azure Functions with Service Bus Trigger.
 */
public class OrderReservationFunction {

    private static final String connection = System.getenv("AZURE_STORAGE_CONNECTION");
    private static final String containerName = System.getenv("AZURE_STORAGE_CONTAINER_NAME");
    private static final BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connection).buildClient();

    @FunctionName("OrderItemReservation")
    @StorageAccount("AzureWebJobsStorage")
    @FixedDelayRetry(maxRetryCount = 3, delayInterval = "00:00:10")
    public void serviceBusProcess(
            @ServiceBusQueueTrigger(name = "orders",
                    queueName = "orders",
                    connection = "AzureWebJobsServiceBus") OrderReservationRequest message,
            final ExecutionContext context
    ) {
        context.getLogger().info("Java Service Bus Queue trigger function processed a message: " + message);

        var sessionId = message.getSessionId();
        var payload = message.getPayload();

        context.getLogger().info("Session ID: " + sessionId);
        context.getLogger().info("Order Reservation payload: " + payload);

        context.getLogger().info("Trying to save Order json in Blob Storage.");

        // Save order json to blob storage
        try {
            var blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient blobClient = blobContainerClient.getBlobClient(sessionId + ".json");
            byte[] blobData = payload.getBytes(StandardCharsets.UTF_8);
            blobClient.upload(new ByteArrayInputStream(blobData), blobData.length, true);
        } catch (Exception e) {
            context.getLogger().severe("Error during uploading to Blob Storage: " + e);
            throw new RuntimeException("Failed upload to the storage", e);
        }
    }
}
