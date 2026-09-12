package com.rupeek.poc.temporal.activity;

import io.temporal.activity.DynamicActivity;
import io.temporal.common.converter.EncodedValues;

/**
 * One implementation, N activity type names.
 *
 * The workflow invokes calls through an untyped stub using the config's call
 * name as the activity TYPE, so the Temporal UI timeline reads
 * "PRECHECK.nameDobCheck", "PRECHECK.panValidation", "FUND_TRANSFER.disburse"
 * instead of a stack of identical "Execute" boxes. Every unregistered activity
 * type on this worker lands here and is delegated to the single generic executor.
 *
 * loadConfig stays a normal typed activity — it is one call, its name is already
 * meaningful, and keeping it typed keeps the config contract compile-checked.
 */
public class DynamicStageActivityImpl implements DynamicActivity {

    private final StageActivity delegate;

    public DynamicStageActivityImpl() {
        this(new StageActivityImpl());
    }

    /** Test seam: lets the suite point the dynamic dispatcher at a fake executor. */
    public DynamicStageActivityImpl(StageActivity delegate) {
        this.delegate = delegate;
    }

    @Override
    public Object execute(EncodedValues args) {
        StageActivity.CallRequest req = args.get(0,
                StageActivity.CallRequest.class, StageActivity.CallRequest.class);
        return delegate.execute(req);
    }
}
