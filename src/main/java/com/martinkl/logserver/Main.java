package com.martinkl.logserver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private final StreamStore store;
    private final Server server;


    @SuppressWarnings("serial")
    public class RootServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("<!DOCTYPE html>");
            response.getWriter().println("<p>Hello world</p>");
        }
    }

    public class EventSocket extends WebSocketAdapter {
        private final String streamId;
        private final String senderId;
        private int seqNo;

        public EventSocket(String streamId, String senderId, int initialSeqNo) {
            this.streamId = streamId;
            this.senderId = senderId;
            this.seqNo = initialSeqNo;
        }

        @Override
        public void onWebSocketConnect(Session session) {
            super.onWebSocketConnect(session);
            log.info("Socket connected on stream {}: {}", streamId, session);
        }

        @Override
        public void onWebSocketBinary(byte[] payload, int offset, int len) {
            super.onWebSocketBinary(payload, offset, len);
            StreamKey key = new StreamKey(streamId, senderId, seqNo);
            seqNo++;

            StringBuilder str = new StringBuilder("Received BINARY message: ");
            str.append(key);
            str.append(" value =");
            for (int i = 0; i < len; i++) {
                str.append(' ');
                int val = payload[offset + i];
                if (val < 0) val += 256; // signed to unsigned
                if (val < 16) str.append('0');
                str.append(Integer.toHexString(val));
            }
            log.info(str.toString());

            store.publishEvent(key, Arrays.copyOfRange(payload, offset, offset + len));

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
            log.info("Received TEXT message on stream {}: {}", streamId, message);
        }

        @Override
        public void onWebSocketClose(int statusCode, String reason) {
            super.onWebSocketClose(statusCode, reason);
            log.info("Socket for stream {} closed: {}", streamId, Integer.toString(statusCode) + " " + reason);
        }

        @Override
        public void onWebSocketError(Throwable cause) {
            super.onWebSocketError(cause);
            log.info("Socket error: ", cause);
        }
    }

    private class EventSocketCreator implements WebSocketCreator {
        private final Pattern STREAM_PARAM = Pattern.compile("\\A[0-9a-fA-F]{32}\\z");
        private final Pattern SENDER_PARAM = Pattern.compile("\\A[0-9a-fA-F]{16}\\z");
        private final Pattern SEQ_NO_PARAM = Pattern.compile("\\A(0|[1-9][0-9]{0,8})\\z");

        @Override
        public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
            String streamId = getParameter(req, resp, "stream", STREAM_PARAM);
            if (streamId == null) return null;
            String senderId = getParameter(req, resp, "sender", SENDER_PARAM);
            if (senderId == null) return null;
            String seqNoStr = getParameter(req, resp, "seqno", SEQ_NO_PARAM);
            if (seqNoStr == null) return null;

            return new EventSocket(streamId, senderId, Integer.parseInt(seqNoStr));
        }

        public String getParameter(ServletUpgradeRequest req, ServletUpgradeResponse resp,
                                   String paramName, Pattern expectedRegex) {
            List<String> values = req.getParameterMap().get(paramName);
            if (values == null || values.size() != 1) {
                badRequest(resp, "Missing query parameter " + paramName);
                return null;
            }

            String value = values.get(0);
            if (!expectedRegex.matcher(value).matches()) {
                badRequest(resp, "Malformed query parameter " + paramName);
                return null;
            }
            return value;
        }

        private void badRequest(ServletUpgradeResponse resp, String message) {
            try {
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST, message);
            } catch (IOException e) {
                log.info("Error sending error message on upgrade: ", e);
            }
        }
    }

    @SuppressWarnings("serial")
    public class EventServlet extends WebSocketServlet {
        @Override
        public void configure(WebSocketServletFactory factory) {
            factory.getPolicy().setIdleTimeout(600000);
            factory.setCreator(new EventSocketCreator());
        }
    }

    public Main() {
        this.store = new StreamStore();
        this.server = new Server(8080);
    }

    public void run() throws Exception {
        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new RootServlet()), "/");
        handler.addServlet(new ServletHolder(new EventServlet()), "/events");

        server.setHandler(handler);
        server.start();
        store.run();
    }

    public static void main(String[] args) throws Exception {
        new Main().run();
    }
}
