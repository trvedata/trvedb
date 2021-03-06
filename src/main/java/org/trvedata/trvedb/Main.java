package org.trvedata.trvedb;

import org.trvedata.trvedb.rest.ChannelResource;
import org.trvedata.trvedb.storage.StreamStore;
import org.trvedata.trvedb.websocket.EventsServlet;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

/**
 * Entry point for running the log server as a standalone process.
 */
public class Main extends Application<LogServerConfig> {

    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    @Override
    public String getName() {
        return "logserver";
    }

    @Override
    public void initialize(Bootstrap<LogServerConfig> bootstrap) {
        bootstrap.addBundle(new AssetsBundle("/web-ui/", "/", "index.html", "web-ui"));
    }

    @Override
    public void run(LogServerConfig configuration, Environment environment) throws Exception {
        StreamStore store = new StreamStore();

        environment.lifecycle().manage(store);
        environment.healthChecks().register("logserver", new LogServerHealthCheck());
        environment.jersey().register(new ChannelResource());
        environment.servlets().addServlet("events", new EventsServlet(store)).addMapping("/events");
    }
}
