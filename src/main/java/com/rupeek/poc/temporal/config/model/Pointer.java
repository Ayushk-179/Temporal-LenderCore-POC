package com.rupeek.poc.temporal.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * The ONLY mutable document in the store.
 *
 * Publishing is a pointer flip; rollback is flipping it back. Every other
 * document is immutable and content-versioned, which is what lets a worker cache
 * a version forever and lets history record exactly which version an attempt used.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Pointer {
    public String skeletonVersion = "v1";
    public String bindingVersion = "b1";
    public String transportVersion = "t1";
}
