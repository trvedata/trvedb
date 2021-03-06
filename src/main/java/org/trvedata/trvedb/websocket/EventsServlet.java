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
 * The endpoint accepts a query parameter identifying the client's peer ID
 * (currently not authenticated, but the plan is to authenticate it in future).
 * For every established WebSocket connection, this servlet creates an
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
        private final Pattern PEER_ID_PARAM = Pattern.compile("\\A[0-9a-fA-F]{64}\\z");

        @Override
        public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
            String peerID = getParameter(req, resp, "peer_id", PEER_ID_PARAM);
            if (peerID == null) return null;
            return new EventsConnection(store, peerID);
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
