package org.trvedata.trvedb;

import org.hibernate.validator.constraints.NotEmpty;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

public class LogServerConfig extends Configuration {
    @NotEmpty
    private String bootstrapServer = "localhost:9092";

    @NotEmpty
    private String kafkaTopic = "events";

    @JsonProperty
    public String getBootstrapServer() {
        return bootstrapServer;
    }

    @JsonProperty
    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    @JsonProperty
    public String getKafkaTopic() {
        return kafkaTopic;
    }

    @JsonProperty
    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }
}
