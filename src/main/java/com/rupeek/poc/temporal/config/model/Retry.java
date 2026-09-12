package com.rupeek.poc.temporal.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Retry {
    public int initialIntervalSeconds = 2;
    public double backoffCoefficient = 2.0;
    public int maximumAttempts = 3;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> nonRetryableErrorTypes = new ArrayList<>();
}
