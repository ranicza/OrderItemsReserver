package com.petstore.demo;

import java.io.Serializable;

public class OrderReservationRequest implements Serializable {
    private String sessionId;
    private String payload;

    public OrderReservationRequest() {
    }

    public OrderReservationRequest(String sessionId, String payload) {
        this.sessionId = sessionId;
        this.payload = payload;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
