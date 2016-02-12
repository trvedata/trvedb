package org.trvedata.trvedb.rest;

import org.hibernate.validator.constraints.Length;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ChannelModel {
    @Length(min = 32, max = 32)
    private String channelID;

    public ChannelModel() {
    }

    public ChannelModel(String channelID) {
        this.channelID = channelID;
    }

    @JsonProperty
    public String getChannelID() {
        return channelID;
    }
}
