package com.example.swim;

import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.XAddParams;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class JmsBridgeApplication {
    public static void main(String[] args) throws Exception {
        BridgeConfig config = BridgeConfig.fromEnv();
        JedisPooled jedis = new JedisPooled(config.redisUrl());

        SolConnectionFactory factory = SolJmsUtility.createConnectionFactory();
        factory.setHost(config.host());
        factory.setVPN(config.vpn());
        factory.setUsername(config.username());
        factory.setPassword(config.password());
        if (config.clientName() != null && !config.clientName().isBlank()) {
            factory.setClientID(config.clientName());
        }

        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        if (config.queueName() == null || config.queueName().isBlank()) {
            throw new IllegalArgumentException("SOLACE_QUEUE_NAME is required");
        }
        Queue queue = session.createQueue(config.queueName());
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(
                new RedisForwardingListener(jedis, config.redisStream(), config.redisStreamMaxLen(), config.queueName())
        );

        connection.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                connection.close();
                jedis.close();
            } catch (Exception ignored) {
            }
        }));

        System.out.println("JMS bridge connected. Queue=" + config.queueName());
        Thread.currentThread().join();
    }

    record BridgeConfig(String host, String vpn, String username, String password, String clientName,
                        String redisUrl, String redisStream, long redisStreamMaxLen, String queueName) {
        static BridgeConfig fromEnv() {
            return new BridgeConfig(
                    getenv("SOLACE_HOST", ""),
                    getenv("SOLACE_VPN", ""),
                    getenv("SOLACE_USERNAME", ""),
                    getenv("SOLACE_PASSWORD", ""),
                    getenv("SOLACE_CLIENT_NAME", "faa-swim-jms-bridge"),
                    getenv("REDIS_URL", "redis://localhost:6379"),
                    getenv("RAW_STREAM_NAME", "faa.raw.xml"),
                    Long.parseLong(getenv("RAW_STREAM_MAXLEN", "200000")),
                    getenv("SOLACE_QUEUE_NAME", "")
            );
        }

        private static String getenv(String key, String fallback) {
            String value = System.getenv(key);
            return value == null || value.isBlank() ? fallback : value;
        }
    }

    static class RedisForwardingListener implements MessageListener {
        private final JedisPooled jedis;
        private final String streamName;
        private final long streamMaxLen;
        private final String queueName;

        RedisForwardingListener(JedisPooled jedis, String streamName, long streamMaxLen, String queueName) {
            this.jedis = jedis;
            this.streamName = streamName;
            this.streamMaxLen = streamMaxLen;
            this.queueName = queueName;
        }

        @Override
        public void onMessage(Message message) {
            try {
                String xml = extractBody(message);
                Map<String, String> fields = new HashMap<>();
                fields.put("xml", xml);
                fields.put("source", "solace-jms");
                fields.put("queue", queueName);
                fields.put("jms_message_id", safe(() -> message.getJMSMessageID()));
                fields.put("jms_timestamp", String.valueOf(safeLong(() -> message.getJMSTimestamp())));
                fields.put("bridge_received_at", Instant.now().toString());
                jedis.xadd(streamName, fields, XAddParams.xAddParams().maxLen(streamMaxLen));
                message.acknowledge();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private String extractBody(Message message) throws JMSException {
            if (message instanceof TextMessage textMessage) {
                return textMessage.getText();
            }
            if (message instanceof BytesMessage bytesMessage) {
                bytesMessage.reset();
                byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
                bytesMessage.readBytes(bytes);
                return new String(bytes, StandardCharsets.UTF_8);
            }
            throw new IllegalArgumentException("Unsupported JMS message type: " + message.getClass().getName());
        }

        private interface ThrowingSupplier<T> { T get() throws JMSException; }
        private static String safe(ThrowingSupplier<String> supplier) { try { return supplier.get(); } catch (Exception e) { return ""; } }
        private static long safeLong(ThrowingSupplier<Long> supplier) { try { return supplier.get(); } catch (Exception e) { return 0L; } }
    }
}
