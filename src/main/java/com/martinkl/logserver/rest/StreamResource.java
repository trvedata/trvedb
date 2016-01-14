package com.martinkl.logserver.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import com.google.common.base.Optional;

@Path("/streams")
@Produces(MediaType.APPLICATION_JSON)
public class StreamResource {
    @GET
    public StreamModel getStream(@QueryParam("stream") Optional<String> streamId) {
        return new StreamModel(streamId.or("00000000000000000000000000000000"));
    }
}
