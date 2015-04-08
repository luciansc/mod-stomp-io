package com.jetdrone.vertx.mods.stomp;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;

import com.jetdrone.vertx.mods.stomp.impl.FrameHandler;
import com.jetdrone.vertx.mods.stomp.impl.StompSubscriptions;

/**
 * STOMP StompClient Class
 * <p>
 * All STOMP protocol is exposed as methods of this class (`connect()`, `send()`, etc.)
 * </p>
 */
public class StompClient implements FrameHandler {

    private static class Heartbeat {
        int sx;
        int sy;

        static Heartbeat parse(String header) {
            String[] token = header.split(",");
            Heartbeat beat = new Heartbeat();
            beat.sx = Integer.parseInt(token[0]);
            beat.sy = Integer.parseInt(token[1]);

            return beat;
        }

        @Override
        public String toString() {
            return sx + "," + sy;
        }
    }

    private static final Frame ASYNC_FRAME = new Frame("ASYNC");

    private final Heartbeat heartbeat = new Heartbeat();
    private final Queue<Handler<Frame>> replies = new LinkedList<>();
    private final StompSubscriptions subscriptions;

    private long pinger;
    private long ponger;
    private long serverActivity;

    private final Vertx vertx;
    private final Logger logger;
    private final String host;
    private final int port;
    private final String login;
    private final String passcode;

    private NetSocket netSocket;
    final FrameParser frameParser = new FrameParser(this);

    private static enum State {
        DISCONNECTED,
        CONNECTING,
        CONNECTED
    }

    private State state = State.DISCONNECTED;

    private static String getSupportedVersions() {
        return Protocol.V1_2.version + "," + Protocol.V1_1.version + "," + Protocol.V1_0.version;
    }
    
    private final boolean isSSL;
    private final String clientKeyStoreLocation;
    private final String clientKeyStorePassword;
    private final String clientTrustStoreLocation;
    private final String clientTrustStorePassword;
    
    public StompClient(final Vertx vertx, final Logger logger, final boolean isSSL, final String host, final int port,
            final String login, final String passcode, final StompSubscriptions subscriptions,
            final String clientKeyStoreLocation, final String clientKeyStorePassword,
            final String clientTrustStoreLocation, final String clientTrustStorePassword) {
        this.vertx = vertx;
        this.logger = logger;
        this.host = host;
        this.port = port;
        this.login = login;
        this.passcode = passcode;
        this.subscriptions = subscriptions;

        this.isSSL = isSSL;
        this.clientKeyStoreLocation = clientKeyStoreLocation;
        this.clientKeyStorePassword = clientKeyStorePassword;
        this.clientTrustStoreLocation = clientTrustStoreLocation;
        this.clientTrustStorePassword = clientTrustStorePassword;
    }


    public void connect(final AsyncResultHandler<Void> resultHandler) {
        if (state == State.DISCONNECTED) {
            state = State.CONNECTING;

            NetClient client = vertx.createNetClient();
            if (isSSL) {
                client.setSSL(isSSL);
                client.setKeyStorePassword(clientKeyStorePassword);
                client.setKeyStorePath(clientKeyStoreLocation);
                client.setTrustStorePassword(clientTrustStorePassword);
                client.setTrustStorePath(clientTrustStoreLocation);
            }

            client.connect(port, host, new AsyncResultHandler<NetSocket>() {
                @Override
                public void handle(final AsyncResult<NetSocket> asyncResult) {
                    if (asyncResult.failed()) {
                        logger.error("Net client error", asyncResult.cause());
                        if (resultHandler != null) {
                            resultHandler.handle(new AsyncResult<Void>() {

                                @Override
                                public Void result() {
                                    return null;
                                }

                                @Override
                                public Throwable cause() {
                                    return asyncResult.cause();
                                }

                                @Override
                                public boolean succeeded() {
                                    return asyncResult.succeeded();
                                }

                                @Override
                                public boolean failed() {
                                    return asyncResult.failed();
                                }
                            });
                        }
                        disconnect();
                    } else {
                        state = State.CONNECTED;
                        netSocket = asyncResult.result();
                        init(netSocket);
                        netSocket.exceptionHandler(new Handler<Throwable>() {
                            public void handle(Throwable e) {
                                logger.error("Socket client error", e);
                                disconnect();
                            }
                        });
                        netSocket.closeHandler(new Handler<Void>() {
                            public void handle(Void arg0) {
                                logger.info("Socket closed");
                                disconnect();
                            }
                        });
                        if (resultHandler != null) {
                            resultHandler.handle(new AsyncResult<Void>() {

                                @Override
                                public Void result() {
                                    return null;
                                }

                                @Override
                                public Throwable cause() {
                                    return null;
                                }

                                @Override
                                public boolean succeeded() {
                                    return true;
                                }

                                @Override
                                public boolean failed() {
                                    return false;
                                }
                            });
                        }
                    }
                }
            });
        }
    }

    /**
     * Clean up client resources when it is disconnected or the server did not
     * send heart beats in a timely fashion
     */
    private void disconnect() {
        state = State.DISCONNECTED;
        if (pinger != 0) {
            vertx.cancelTimer(pinger);
        }
        if (ponger != 0) {
            vertx.cancelTimer(ponger);
        }
        // make sure the socket is closed
        if (netSocket != null) {
            netSocket.close();
        }
    }

    void send(final Frame frame, final boolean async, final Handler<Frame> replyHandler) {
        switch (state) {
            case CONNECTED:
                netSocket.write(frame.command);
                netSocket.write("\n");

                for (Map.Entry<String, String> entry : frame.headers.entrySet()) {
                    String value = entry.getValue();
                    if (value != null) {
                        netSocket.write(entry.getKey());
                        netSocket.write(":");
                        netSocket.write(Frame.escape(entry.getValue()));
                        netSocket.write("\n");
                    }
                }

                if (frame.body != null) {
                    netSocket.write("content-length:");
                    netSocket.write(Integer.toString(frame.body.length()));
                    netSocket.write("\n");
                }

                netSocket.write("\n");
                if (frame.body != null) {
                    netSocket.write(frame.body);
                }

                netSocket.write("\0");
                if (async) {
                    replyHandler.handle(ASYNC_FRAME);
                } else {
                    replies.offer(replyHandler);
                }
                break;
            case DISCONNECTED:
                logger.info("Got request when disconnected. Trying to connect.");
                connect(new AsyncResultHandler<Void>() {
                    public void handle(AsyncResult<Void> connection) {
                        if (connection.succeeded()) {
                            send(frame, async, replyHandler);
                        } else {
                            Frame error = new Frame("ERROR");
                            error.body = "Unable to connect";
                            replyHandler.handle(error);
                        }
                    }
                });
                break;
            case CONNECTING:
                logger.debug("Got send request while connecting. Will try again in a while.");
                vertx.setTimer(100, new Handler<Long>() {
                    public void handle(Long event) {
                        send(frame, async, replyHandler);
                    }
                });
        }
    }


    private void init(NetSocket netSocket) {
        this.netSocket = netSocket;
        // send heartbeat every 10s by default (value is in ms)
        heartbeat.sx = 10000;
        // expect to receive server heartbeat at least every 10s by default (value in ms)
        heartbeat.sy = 10000;
        // setup the handlers
        netSocket.dataHandler(new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {
//                System.out.println("<<<" + buffer.toString("UTF-8").replaceAll("\0", "^@"));
                serverActivity = System.currentTimeMillis();
                frameParser.handle(buffer);
            }
        });
        // perform the connect command
        logger.debug("Socket Opened...");
        Frame connect = new Frame("CONNECT");
        connect.putHeader("accept-version", getSupportedVersions());
        connect.putHeader("heart-beat", heartbeat.toString());
        connect.putHeader("vhost", host);
        if (login != null) {
            connect.putHeader("login", login);
        }
        if (passcode != null) {
            connect.putHeader("passcode", passcode);
        }

        send(connect, false, new Handler<Frame>() {
            @Override
            public void handle(Frame frame) {
                logger.debug("connected to server " + frame.headers.get("server"));
                // connected = true
                setupHeartbeat(frame.headers);
            }
        });
    }

    private void setupHeartbeat(Map<String, String> headers) {
        if (headers.get("version").equals(Protocol.V1_0.version)) {
            return;
        }

        // heart-beat header received from the server looks like:
        //
        //     heart-beat: sx, sy
        Heartbeat heartbeat = Heartbeat.parse(headers.get("heart-beat"));

        if (this.heartbeat.sx != 0 && heartbeat.sy != 0) {
            final int ttl = Math.max(this.heartbeat.sx, heartbeat.sy);
            logger.debug("send PING every " + ttl + "ms");
            this.pinger = vertx.setPeriodic(ttl, new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    if (state == State.CONNECTED) {
                        logger.debug("PING");
                        netSocket.write("\n");
                    }
                }
            });
        }

        if (this.heartbeat.sy != 0 && heartbeat.sx != 0) {
            final int ttl = Math.max(this.heartbeat.sy, heartbeat.sx);
            logger.debug("check PONG every " + ttl + "ms");
            ponger = vertx.setPeriodic(ttl, new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    long delta = System.currentTimeMillis() - serverActivity;
                    // We wait twice the TTL to be flexible on window's setInterval calls
                    if (delta > ttl * 2) {
                        logger.debug("did not receive server activity for the last " + delta + "ms");
                        disconnect();
                    }
                }
            });
        }
    }

    @Override
    public void handleFrame(Frame reply) {
        if ("ERROR".equals(reply.command)) {
            logger.error(reply.body);
            disconnect();
            return;
        }

        Handler<Frame> handler = replies.poll();

        if (handler != null) {
            // handler waits for this response
            handler.handle(reply);
            return;
        }

        // this is a subscribe message
        if ("MESSAGE".equals(reply.command)) {
            handler = subscriptions.getHandler(reply.headers.get("subscription"));
            if (handler != null) {
                // pub sub handler exists
                handler.handle(reply);
                return;
            }

        }

//        System.out.println(reply.command);
//        System.out.println(reply.toJSON());

        throw new RuntimeException("Received a non MESSAGE while in SUBSCRIBE mode");
    }
}
