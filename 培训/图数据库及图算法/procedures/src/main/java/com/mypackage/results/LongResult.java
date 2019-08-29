package com.mypackage.results;

public class LongResult {
    public static final LongResult NULL = new LongResult(null);
    public final Long value;

    public LongResult(Long value) {
        this.value = value;
    }
}
