package com.martinkl.logserver.rest;

import org.hibernate.validator.constraints.Length;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamModel {
    @Length(min = 32, max = 32)
    private String streamId;

    public StreamModel() {
    }

    public StreamModel(String streamId) {
        this.streamId = streamId;
    }

    @JsonProperty
    public String getStreamId() {
        return streamId;
    }
}
