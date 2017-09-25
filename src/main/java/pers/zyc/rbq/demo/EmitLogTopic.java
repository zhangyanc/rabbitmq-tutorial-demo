package pers.zyc.rbq.demo;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * @author zhangyancheng
 */
public class EmitLogTopic {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmitLogTopic.class);
    private static final String EXCHANGE_NAME = "topic_logs";
    //private static final String[] ROUTING_KEYS = {"*.orange.*", "*.*.rabbit", "lazy.#"};

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
        for (String routingKey : Arrays.asList("quick.orange.rabbit",
                                               "lazy.orange.elephant",
                                               "quick.orange.fox",
                                               "quick.brown.fox",
                                               "lazy.pink.rabbit",
                                               "orange",
                                               "quick.orange.male.rabbit",
                                               "lazy.orange.male.rabbit")) {
            channel.basicPublish(EXCHANGE_NAME, routingKey, null, routingKey.getBytes("UTF-8"));
            LOGGER.info("[x] send '{}'", routingKey);
        }

        channel.close();
        connection.close();
    }
}
