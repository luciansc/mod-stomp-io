package com.jetdrone.vertx.mods.stomp;

import com.jetdrone.vertx.mods.stomp.impl.StompSubscriptions;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.Random;
import java.util.UUID;

public class StompClientBusMod extends BusModBase implements Handler<Message<JsonObject>> {

    private static final Random RANDOM = new Random();
    
    private static final String STOMP_TRANSPORT_CONNECTOR = "stomp";
    private static final String STOMP_SSL_TRANSPORT_CONNECTOR = "stomp+ssl";
    private static final String CONF_TRANSPORT_CONNECTOR_KEY = "transportConnector";
    private static final int CONF_PORT_STOMP_DEFAULT_VALUE = 61613;
    private static final int CONF_PORT_STOMP_SSL_DEFAULT_VALUE = 61612;
    
    private static final String CONF_CLIENT_KEY_STORE_LOCATION = "clientKeyStoreLocation";
    private static final String CONF_CLIENT_KEY_STORE_PASSWORD = "clientKeyStorePassword";
    private static final String CONF_CLIENT_TRUST_STORE_LOCATION = "clientTrustStoreLocation";
    private static final String CONF_CLIENT_TRUST_STORE_PASSWORD = "clientTrustStorePassword";
    
    private static String generateID() {
        return new UUID(RANDOM.nextLong(), RANDOM.nextLong()).toString();
    }

    private static String getRequiredField(String name, Message<JsonObject> msg) throws RuntimeException {
        String field = msg.body().getString(name);
        if (field == null) {
            throw new RuntimeException("Field: <" + name + "> is required!");
        }
        return field;
    }
    
    private StompClient stompClient;
    private StompSubscriptions stompSubscriptions;
    private String baseAddress;

    @Override
    public void start() {
        super.start();

        final String connectorProtocol = getOptionalStringConfig(StompClientBusMod.CONF_TRANSPORT_CONNECTOR_KEY,
                StompClientBusMod.STOMP_TRANSPORT_CONNECTOR);
        boolean isSSL = StompClientBusMod.STOMP_SSL_TRANSPORT_CONNECTOR.equals(connectorProtocol);

        String host = getOptionalStringConfig("host", "localhost");
        final int port = getOptionalIntConfig("port", isSSL ? StompClientBusMod.CONF_PORT_STOMP_SSL_DEFAULT_VALUE
                : StompClientBusMod.CONF_PORT_STOMP_DEFAULT_VALUE);
        String login = getOptionalStringConfig("login", null);
        String passcode = getOptionalStringConfig("passcode", null);
        
        final String clientKeyStoreLocation = getOptionalStringConfig(CONF_CLIENT_KEY_STORE_LOCATION, "");
        final String clientKeyStorePassword = getOptionalStringConfig(CONF_CLIENT_KEY_STORE_PASSWORD, "");
        final String clientTrustStoreLocation = getOptionalStringConfig(CONF_CLIENT_TRUST_STORE_LOCATION, "");
        final String clientTrustStorePassword = getOptionalStringConfig(CONF_CLIENT_TRUST_STORE_PASSWORD, "");

        stompSubscriptions = new StompSubscriptions();

        stompClient = new StompClient(vertx, logger, isSSL, host, port, login, passcode, stompSubscriptions,
                clientKeyStoreLocation, clientKeyStorePassword, clientTrustStoreLocation, clientTrustStorePassword);
        stompClient.connect(null);

        baseAddress = getOptionalStringConfig("address", "vertx.mod-stomp-io");
        eb.registerHandler(baseAddress, this);
    }

    @Override
    public void stop() {
        stompClient.send(new Frame("DISCONNECT"), true, new Handler<Frame>() {
            @Override
            public void handle(Frame event) {
                // NOOP
            }
        });
    }

    @Override
    public void handle(final Message<JsonObject> message) {

        String command = message.body().getString("command");

        if (command == null) {
            sendError(message, "command must be specified");
            return;
        }

        final Frame frame = new Frame(command);
        JsonObject headers;

        try {
            switch (command) {
                case "connect":
                case "disconnect":
                    sendError(message, "This command is not allowed, CONNECT/DISCONNECT is handled by the module");
                    break;
                case "send":
                    frame.headers.put("destination", getRequiredField("destination", message));
                    frame.headers.put("transaction", message.body().getString("transaction"));
                    // add user defined headers
                    headers = message.body().getObject("headers");
                    if (headers != null) {
                        for (String header : headers.getFieldNames()) {
                            Object value = headers.getValue(header);
                            if (value != null) {
                                frame.headers.put(header, value.toString());
                            }
                        }
                    }

                    Object body = message.body().getField("body");

                    if (body != null) {
                        if (body instanceof String) {
                            frame.body = (String) body;
                            frame.headers.put("content-type", "text/plain");
                        }
                        else if (body instanceof JsonObject) {
                            frame.body = ((JsonObject) body).encode();
                            frame.headers.put("content-type", "application/json");
                        }
                    }

                    boolean sync = message.body().getBoolean("sync", false);
                    if (sync) {
                        // if receipt is present a RECEIPT frame is returned, else nothing is
                        frame.headers.put("receipt", generateID());
                        stompClient.send(frame, false, new Handler<Frame>() {
                            @Override
                            public void handle(Frame frame) {
                                // should also include the id in the response
                                sendOK(message, new JsonObject().putString("receipt", frame.headers.get("receipt-id")));
                            }
                        });

                    } else {
                        stompClient.send(frame, true, new Handler<Frame>() {
                            @Override
                            public void handle(Frame event) {
                                sendOK(message, null);
                            }
                        });
                    }
                    break;
                case "subscribe":
                    // for convenience if the `id` header is not set, we create a new one for this client
                    // that will be returned to be able to unsubscribe this subscription
                    final String subscribeId = message.body().getString("id", generateID());
                    String destination = getRequiredField("destination", message);
                    frame.headers.put("id", subscribeId);
                    frame.headers.put("destination", destination);
                    frame.headers.put("ack", message.body().getString("ack", "auto"));

                    // add user defined headers
                    headers = message.body().getObject("headers");
                    if (headers != null) {
                        for (String header : headers.getFieldNames()) {
                            Object value = headers.getValue(header);
                            if (value != null) {
                                frame.headers.put(header, value.toString());
                            }
                        }
                    }

                    // compose the listening address as base + destination
                    final String vertxChannel = baseAddress + destination;
                    stompSubscriptions.registerSubscribeHandler(subscribeId, new Handler<Frame>() {
                        @Override
                        public void handle(Frame reply) {
                            JsonObject replyMessage = new JsonObject();
                            replyMessage.putString("status", "ok");
                            replyMessage.putObject("value", reply.toJSON());
                            eb.send(vertxChannel, replyMessage);
                        }
                    });

                    stompClient.send(frame, true, new Handler<Frame>() {
                        @Override
                        public void handle(Frame event) {
                            sendOK(message, new JsonObject().putString("id", subscribeId));
                        }
                    });
                    break;
                case "unsubscribe":
                    final String unsubscribeId = getRequiredField("id", message);
                    frame.headers.put("id", unsubscribeId);
                    stompClient.send(frame, true, new Handler<Frame>() {
                        @Override
                        public void handle(Frame event) {
                            stompSubscriptions.unregisterSubscribeHandler(unsubscribeId);
                            sendOK(message);
                        }
                    });
                    break;
                case "ack":
                case "nack":
                    frame.headers.put("id", getRequiredField("id", message));
                    frame.headers.put("transaction", message.body().getString("transaction"));

                    stompClient.send(frame, true, new Handler<Frame>() {
                        @Override
                        public void handle(Frame event) {
                            sendOK(message, null);
                        }
                    });
                    break;
                case "begin":
                    // for convenience if the `transaction` header is not set, we create a new one for this client
                    // that will be returned to be able to commit/abort/send this tx
                    frame.headers.put("transaction", message.body().getString("transaction", generateID()));
                    stompClient.send(frame, true, new Handler<Frame>() {
                        @Override
                        public void handle(Frame event) {
                            sendOK(message, new JsonObject().putString("transaction", frame.headers.get("transaction")));
                        }
                    });
                    break;
                case "commit":
                case "abort":
                    frame.headers.put("transaction", getRequiredField("transaction", message));
                    stompClient.send(frame, true, new Handler<Frame>() {
                        @Override
                        public void handle(Frame event) {
                            sendOK(message, null);
                        }
                    });
                    break;
                default:
                    sendError(message, "Invalid command: " + command);
            }
        } catch (RuntimeException ex) {
            sendError(message, ex.getMessage());
        }
    }
}
