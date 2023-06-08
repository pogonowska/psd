package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class OutputMessage {
    @JsonProperty("card_id")
    int card_id;
    @JsonProperty("user_id")
    int user_id;
    @JsonProperty("latitude")
    double latitude;
    @JsonProperty("longitude")
    double longitude;
    @JsonProperty("transaction_value")
    double value;
    @JsonProperty("account_limit")
    double limit;
    @JsonProperty("sentAt")
    String sentAt;
    @JsonProperty("Reason")
    String reason;

    public OutputMessage(InputMessage inputMessage, String reason){
        this.card_id = inputMessage.card_id;
        this.user_id = inputMessage.user_id;
        this.latitude = inputMessage.latitude;
        this.longitude = inputMessage.longitude;
        this.value = inputMessage.value;
        this.limit = inputMessage.limit;
        this.sentAt = inputMessage.sentAt;
        this.reason = reason;
    }

    public OutputMessage(Tuple2<String, Integer> inputMessage, String reason){
        this.card_id = Integer.parseInt(inputMessage.f0);
        this.reason = reason;
    }
}
