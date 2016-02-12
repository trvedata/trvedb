package org.trvedata.trvedb.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import com.google.common.base.Optional;

@Path("/channels")
@Produces(MediaType.APPLICATION_JSON)
public class ChannelResource {
    @GET
    public ChannelModel getChannel(@QueryParam("id") Optional<String> channelID) {
        return new ChannelModel(channelID.or("00000000000000000000000000000000"));
    }
}
