package com.martinkl.logserver;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import com.martinkl.logserver.storage.StreamStore;
import com.martinkl.logserver.websocket.EventsServlet;

/**
 * Entry point for running the log server as a standalone process.
 */
public class Main {
    private final StreamStore store;
    private final Server server;

    public Main() {
        this.store = new StreamStore();
        this.server = new Server(8080);
    }

    public void run() throws Exception {
        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new RootServlet()), "/");
        handler.addServlet(new ServletHolder(new EventsServlet(store)), "/events");

        server.setHandler(handler);
        server.start();
        store.run();
    }

    public static void main(String[] args) throws Exception {
        new Main().run();
    }
}
