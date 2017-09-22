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
public class EmitLogDirect {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmitLogDirect.class);
    private static final String EXCHANGE_NAME = "direct_logs";
    private static final String[] SEVERITY = {"info", "warn", "error"};

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "direct");

        String severity;
        String message;
        int counts = 10;
        while (counts-- > 0) {
            severity = getSeverity();
            message = "message to direct_logs exchange - " + (10 - counts);
            channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes("UTF-8"));
            LOGGER.info("[x] send '{}':'{}'", severity, message);
            Thread.sleep(2000);
        }
        channel.close();
        connection.close();
    }

    private static String getSeverity() {
        return SEVERITY[(int) (Math.random() * 10000) % 3];
    }
}
