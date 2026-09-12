package com.rupeek.poc.temporal.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Lender-level wire settings. Activity-side, so baseUrl and header rotation are
 * hot: a lender moving /v1 -> /v2 is a one-line edit that reaches loans already
 * parked mid-flight.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Transport {
    public String baseUrl;
    public int defaultTimeoutSeconds = 10;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> staticHeaders = new LinkedHashMap<>();
}
