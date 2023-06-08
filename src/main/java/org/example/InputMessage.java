package org.example;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;

@JsonSerialize
public class InputMessage {
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
}
