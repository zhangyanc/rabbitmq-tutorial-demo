package pers.zyc.rbq.demo;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author zhangyancheng
 */
public class NewTask {
    private final static Logger LOGGER = LoggerFactory.getLogger(NewTask.class);
    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

        String message = getMessage(args);
        channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
        LOGGER.info("[x] send '{}'", message);

        channel.close();
        connection.close();
    }

    private static String getMessage(String[] argsLine) {
        return argsLine.length < 1 ? "Hello World!" : joinStrings(argsLine, " ");
    }

    private static String joinStrings(String[] argsLine, String delimiter) {
        StringBuilder wordsBuf = new StringBuilder(argsLine[0]);
        for (int i = 1; i < argsLine.length; i++) {
            wordsBuf.append(delimiter).append(argsLine[i]);
        }
        return wordsBuf.toString();
    }
}
