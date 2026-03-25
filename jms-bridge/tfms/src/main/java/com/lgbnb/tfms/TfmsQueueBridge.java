package com.lgbnb.tfms;

import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.XAddParams;

import javax.jms.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public final class TfmsQueueBridge {
    private static final Logger log = LoggerFactory.getLogger(TfmsQueueBridge.class);

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    private static boolean looksLikeXml(String payload) {
        if (payload == null) {
            return false;
        }
        for (int i = 0; i < payload.length(); i++) {
            char c = payload.charAt(i);
            if (Character.isWhitespace(c)) {
                continue;
            }
            return c == '<';
        }
        return false;
    }

    private static String extractBody(Message message) throws JMSException {
        if (message instanceof TextMessage textMessage) {
            return textMessage.getText();
        }
        if (message instanceof BytesMessage bytesMessage) {
            bytesMessage.reset();
            byte[] data = new byte[(int) bytesMessage.getBodyLength()];
            bytesMessage.readBytes(data);
            return new String(data, StandardCharsets.UTF_8);
        }
        throw new IllegalArgumentException("Unsupported JMS message type: " + message.getClass().getName());
    }

    public static void main(String[] args) throws Exception {
        String host = env("SOLACE_HOST", "tcp://localhost:55555");
        String vpn = env("SOLACE_VPN", "default");
        String username = env("SOLACE_USERNAME", "default");
        String password = env("SOLACE_PASSWORD", "default");
        String queueName = env("TFMS_QUEUE_NAME", "landgbnb.gmail.com.TFMS.3b02c856-4246-4fa3-8c92-a7c2a37d2176.OUT");
        String redisUrl = env("REDIS_URL", "redis://localhost:6379");
        String rawStreamName = env("TFMS_RAW_STREAM_NAME", "tfms.raw.xml");
        long rawStreamMaxLen = Long.parseLong(env("TFMS_RAW_STREAM_MAXLEN", "200000"));
        int receiveTimeoutMs = Integer.parseInt(env("JMS_RECEIVE_TIMEOUT_MS", "5000"));
        long reconnectDelayMs = Long.parseLong(env("JMS_RECONNECT_DELAY_MS", "5000"));

        JedisPooled jedis = new JedisPooled(redisUrl);

        while (true) {
            Connection connection = null;
            Session session = null;
            MessageConsumer consumer = null;
            try {
                log.info("Connecting to Solace host={} vpn={} queue={}", host, vpn, queueName);
                SolConnectionFactory connectionFactory = SolJmsUtility.createConnectionFactory();
                connectionFactory.setHost(host);
                connectionFactory.setVPN(vpn);
                connectionFactory.setClientID("lgbnb-tfms-bridge");
                connection = connectionFactory.createConnection(username, password);
                session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                Queue queue = session.createQueue(queueName);
                consumer = session.createConsumer(queue);
                connection.start();
                log.info("Connected. Waiting for messages...");

                while (true) {
                    Message message = consumer.receive(receiveTimeoutMs);
                    if (message == null) {
                        continue;
                    }
                    String xml;
                    try {
                        xml = extractBody(message);
                    } catch (IllegalArgumentException ex) {
                        log.warn("Ignoring unsupported message type: {}", message.getClass().getName());
                        message.acknowledge();
                        continue;
                    }

                    if (xml == null || xml.isBlank()) {
                        log.warn("Ignoring empty XML payload from queue={}", queueName);
                        message.acknowledge();
                        continue;
                    }

                    if (!looksLikeXml(xml)) {
                        String preview = xml.substring(0, Math.min(80, xml.length()));
                        log.warn("Ignoring non-XML payload queue={} preview={}...", queueName, preview);
                        message.acknowledge();
                        continue;
                    }

                    Map<String, String> fields = new HashMap<>();
                    fields.put("xml", xml);
                    fields.put("source", "tfms-solace-jms");
                    fields.put("queue", queueName);
                    fields.put("jms_message_id", safe(() -> message.getJMSMessageID()));
                    fields.put("jms_timestamp", String.valueOf(safeLong(() -> message.getJMSTimestamp())));
                    fields.put("bridge_received_at", Instant.now().toString());
                    jedis.xadd(rawStreamName, fields, XAddParams.xAddParams().maxLen(rawStreamMaxLen));
                    message.acknowledge();
                    log.info("Forwarded message to Redis stream={}", rawStreamName);
                }
            } catch (Exception ex) {
                log.error("Bridge error: {}", ex.getMessage(), ex);
                Thread.sleep(reconnectDelayMs);
            } finally {
                try {
                    if (consumer != null) consumer.close();
                } catch (Exception ignored) {}
                try {
                    if (session != null) session.close();
                } catch (Exception ignored) {}
                try {
                    if (connection != null) connection.close();
                } catch (Exception ignored) {}
            }
        }
    }

    private interface ThrowingSupplier<T> {
        T get() throws JMSException;
    }

    private static String safe(ThrowingSupplier<String> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            return "";
        }
    }

    private static long safeLong(ThrowingSupplier<Long> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            return 0L;
        }
    }
}
