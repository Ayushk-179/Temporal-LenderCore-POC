package com.rupeek.poc.temporal.config.model;

/** What the activity resolves, per attempt: the contract plus its provenance. */
public class ResolvedBinding {
    public final String callKey;
    public final String bindingVersion;
    public final String transportVersion;
    public final CallBinding binding;
    public final Transport transport;

    public ResolvedBinding(String callKey, String bindingVersion, String transportVersion,
                           CallBinding binding, Transport transport) {
        this.callKey = callKey;
        this.bindingVersion = bindingVersion;
        this.transportVersion = transportVersion;
        this.binding = binding;
        this.transport = transport;
    }
}
