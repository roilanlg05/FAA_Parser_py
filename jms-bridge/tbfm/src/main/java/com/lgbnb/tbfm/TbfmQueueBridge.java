package com.lgbnb.tbfm;

import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.XAddParams;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public final class TbfmQueueBridge {
    private static final Logger log = LoggerFactory.getLogger(TbfmQueueBridge.class);

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    private static String extractBody(Message message) throws Exception {
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
        String solaceHost = env("SOLACE_HOST", "tcp://localhost:55555");
        String solaceVpn = env("SOLACE_VPN", "default");
        String solaceUsername = env("SOLACE_USERNAME", "default");
        String solacePassword = env("SOLACE_PASSWORD", "default");
        String queueName = env("TBFM_QUEUE_NAME", "");
        String redisUrl = env("REDIS_URL", "redis://localhost:6379");
        String rawStreamName = env("TBFM_RAW_STREAM_NAME", "tbfm.raw.xml");
        long rawStreamMaxLen = Long.parseLong(env("TBFM_RAW_STREAM_MAXLEN", "200000"));
        int receiveTimeoutMs = Integer.parseInt(env("JMS_RECEIVE_TIMEOUT_MS", "5000"));
        long reconnectDelayMs = Long.parseLong(env("JMS_RECONNECT_DELAY_MS", "5000"));

        JedisPooled jedis = new JedisPooled(redisUrl);

        while (true) {
            Connection connection = null;
            Session session = null;
            MessageConsumer consumer = null;
            try {
                SolConnectionFactory connectionFactory = SolJmsUtility.createConnectionFactory();
                connectionFactory.setHost(solaceHost);
                connectionFactory.setVPN(solaceVpn);
                connectionFactory.setClientID("lgbnb-tbfm-redis-bridge");

                connection = connectionFactory.createConnection(solaceUsername, solacePassword);
                session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                Queue queue = session.createQueue(queueName);
                consumer = session.createConsumer(queue);

                connection.start();
                log.info("Connected to Solace queue={} and Redis stream={}", queueName, rawStreamName);

                while (true) {
                    Message message = consumer.receive(receiveTimeoutMs);
                    if (message == null) {
                        continue;
                    }

                    String xml;
                    try {
                        xml = extractBody(message);
                    } catch (IllegalArgumentException ex) {
                        log.warn("Ignoring unsupported message type={}", message.getClass().getName());
                        message.acknowledge();
                        continue;
                    }

                    if (xml == null || xml.isBlank()) {
                        log.warn("Ignoring empty TBFM XML payload queue={}", queueName);
                        message.acknowledge();
                        continue;
                    }

                    Map<String, String> fields = new HashMap<>();
                    fields.put("xml", xml);
                    fields.put("source", "tbfm-solace-jms");
                    fields.put("queue_name", queueName);
                    fields.put("jms_message_id", message.getJMSMessageID() == null ? "" : message.getJMSMessageID());
                    fields.put("jms_timestamp", String.valueOf(message.getJMSTimestamp()));
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
                    if (consumer != null) {
                        consumer.close();
                    }
                } catch (Exception ignored) {
                }
                try {
                    if (session != null) {
                        session.close();
                    }
                } catch (Exception ignored) {
                }
                try {
                    if (connection != null) {
                        connection.close();
                    }
                } catch (Exception ignored) {
                }
            }
        }
    }
}
