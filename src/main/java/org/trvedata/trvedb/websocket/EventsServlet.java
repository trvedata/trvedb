package org.trvedata.trvedb.websocket;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trvedata.trvedb.storage.StreamStore;

/**
 * Servlet for a HTTP endpoint that can be upgraded to a WebSocket connection.
 * The endpoint accepts query parameters identifying the stream to connect to,
 * the ID of the sender, and the sender's last sequence number. For every
 * established WebSocket connection, this servlet creates an
 * {@link EventsConnection} instance.
 */
@SuppressWarnings("serial")
public class EventsServlet extends WebSocketServlet {

    private static final Logger log = LoggerFactory.getLogger(EventsServlet.class);
    private final StreamStore store;

    public EventsServlet(StreamStore store) {
        this.store = store;
    }

    @Override
    public void configure(WebSocketServletFactory factory) {
        factory.getPolicy().setIdleTimeout(600000);
        factory.setCreator(new ConnectionCreator());
    }

    private class ConnectionCreator implements WebSocketCreator {
        private final Pattern STREAM_PARAM = Pattern.compile("\\A[0-9a-fA-F]{32}\\z");
        private final Pattern SENDER_PARAM = Pattern.compile("\\A[0-9a-fA-F]{64}\\z");
        private final Pattern SEQ_NO_PARAM = Pattern.compile("\\A(0|[1-9][0-9]{0,8})\\z");

        @Override
        public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
            String streamId = getParameter(req, resp, "stream", STREAM_PARAM);
            if (streamId == null) return null;
            String senderId = getParameter(req, resp, "sender", SENDER_PARAM);
            if (senderId == null) return null;
            String seqNoStr = getParameter(req, resp, "seqno", SEQ_NO_PARAM);
            if (seqNoStr == null) return null;

            return new EventsConnection(store, streamId, senderId, Integer.parseInt(seqNoStr));
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
}
