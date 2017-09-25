package pers.zyc.rbq.demo;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * @author zhangyancheng
 */
public class ReceiveLogsTopic {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmitLogTopic.class);
    private static final String EXCHANGE_NAME = "topic_logs";
    //private static final String[] ROUTING_KEYS = {"*.orange.*", "*.*.rabbit", "lazy.#"};

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
        String queueName = channel.queueDeclare().getQueue();

        final String[] rks = {"*.orange.*"};
        //final String[] rks = {"*.*.rabbit", "lazy.#"};
        for (String routingKey : rks) {
            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        }

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                LOGGER.info("[" + Arrays.asList(rks) + "] Received '{}'", message);
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }
}
