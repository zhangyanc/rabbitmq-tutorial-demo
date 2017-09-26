package pers.zyc.rbq.demo;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author zhangyancheng
 */
public class RPCServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RPCServer.class);
    private static final String RPC_QUEUE_NAME = "rpc_queue";

    private static int fib(int n) {
        if (n < 2) {
            return n;
        }
        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        Connection connection = null;
        try {
            connection = connectionFactory.newConnection();
            final Channel channel = connection.createChannel();
            channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
            channel.basicQos(1);

            final Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                            .correlationId(properties.getCorrelationId()).build();
                    String response = "";

                    try {
                        String message = new String(body, "UTF-8");
                        int n = Integer.parseInt(message);

                        LOGGER.info("[.] fib({})", n);

                        response += fib(n);
                    } catch (RuntimeException e) {
                        LOGGER.error("[.] fib calc error!");
                    } finally {
                        channel.basicPublish("", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));
                        channel.basicAck(envelope.getDeliveryTag(), false);

                        synchronized (this) {
                            notify();
                        }
                    }
                }
            };
            channel.basicConsume(RPC_QUEUE_NAME, consumer);

            while (true) {
                synchronized (consumer) {
                    try {
                        consumer.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException | TimeoutException e) {
            LOGGER.error("consume error", e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException ignored) {
                }
            }
        }
    }
}
