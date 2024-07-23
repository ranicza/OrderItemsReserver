package com.petstore.demo;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Azure Functions with HTTP Trigger.
 */
public class OrderReservationFunction {

    private static final String connection = System.getenv("AZURE_STORAGE_CONNECTION");
    private static final BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connection).buildClient();
    private static final BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient("orders");

    @FunctionName("OrderItemReservation")
    @StorageAccount("AzureWebJobsStorage")
    public HttpResponseMessage run(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.GET, HttpMethod.POST},
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<OrderReservationRequest>> request,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed an Order Reservation request.");

        if (!request.getBody().isPresent()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Invalid order request object.")
                    .build();
        }

        var orderRequest = request.getBody().get();
        var sessionId = orderRequest.getSessionId();
        var payload = orderRequest.getPayload();

        if (sessionId == null || payload == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).
                    body("SessionId or payload is missing.")
                    .build();
        }

        context.getLogger().info("Session ID: " + sessionId);
        context.getLogger().info("Order Reservation payload: " + payload);

        context.getLogger().info("Trying to save Order json in Blob Storage.");

        // Save order json to blob storage
        BlobClient blobClient = blobContainerClient.getBlobClient(sessionId + ".json");
        byte[] blobData = payload.getBytes(StandardCharsets.UTF_8);
        blobClient.upload(new ByteArrayInputStream(blobData), blobData.length, true);

        return request.createResponseBuilder(HttpStatus.OK)
                .body("Session Id: " + sessionId + ", payload: " + payload)
                .build();
    }
}
