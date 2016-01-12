package com.martinkl.logserver;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    @SuppressWarnings("serial")
    public static class RootServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("<!DOCTYPE html>");
            response.getWriter().println("<p>Hello world</p>");
        }
    }

    public static class EventSocket extends WebSocketAdapter {
        private static final Logger log = LoggerFactory.getLogger(EventSocket.class);

        @Override
        public void onWebSocketConnect(Session session) {
            super.onWebSocketConnect(session);
            log.info("Socket Connected: " + session);
        }

        @Override
        public void onWebSocketBinary(byte[] payload, int offset, int len) {
            super.onWebSocketBinary(payload, offset, len);
            StringBuilder str = new StringBuilder("Received BINARY message:");
            for (int i = 0; i < len; i++) {
                str.append(' ');
                int val = payload[offset + i];
                if (val < 0) val += 256; // signed to unsigned
                if (val < 16) str.append('0');
                str.append(Integer.toHexString(val));
            }
            log.info(str.toString());

            getSession().getRemote().sendString(str.toString(), new WriteCallback() {
                @Override
                public void writeSuccess() {}

                @Override
                public void writeFailed(Throwable error) {
                    log.info("Sending message failed: ", error);
                }
            });
        }

        @Override
        public void onWebSocketText(String message) {
            super.onWebSocketText(message);
            log.info("Received TEXT message: " + message);
        }

        @Override
        public void onWebSocketClose(int statusCode, String reason) {
            super.onWebSocketClose(statusCode,reason);
            log.info("Socket Closed: [" + statusCode + "] " + reason);
        }

        @Override
        public void onWebSocketError(Throwable cause) {
            super.onWebSocketError(cause);
            log.info("Socket error: ", cause);
        }
    }

    @SuppressWarnings("serial")
    public static class EventServlet extends WebSocketServlet {
        @Override
        public void configure(WebSocketServletFactory factory) {
            factory.register(EventSocket.class);
        }
    }

    public static void main(String[] args) throws Exception {
        Server server = new Server(8080);

        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(RootServlet.class, "/");
        handler.addServlet(EventServlet.class, "/events/*");

        server.setHandler(handler);
        server.start();
        server.join();
    }
}
