package com.rupeek.poc.temporal.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

/**
 * One predicate shape, evaluated against either a response body ({@code path})
 * or the workflow context ({@code ctx}).
 *
 * ops: EQUALS | NOT_EQUALS | IN | NOT_IN | BLANK | NOT_BLANK
 *
 * Lives in both planes: skeleton conditions (ctx only, workflow-side) and
 * binding predicates (response path or ctx, activity-side).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Predicate {
    public String path;
    public String ctx;
    public String op = "EQUALS";
    public String value;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> values = new ArrayList<>();
}
