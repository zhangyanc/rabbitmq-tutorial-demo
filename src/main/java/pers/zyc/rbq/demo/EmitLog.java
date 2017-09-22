package pers.zyc.rbq.demo;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author zhangyancheng
 */
public class EmitLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmitLog.class);
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        String message;
        int counts = 10;
        while (counts-- > 0) {
            message = "message to logs exchange - " + (10 - counts);
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
            LOGGER.info("[x] send '{}'", message);
            Thread.sleep(2000);
        }
        channel.close();
        connection.close();
    }
}
