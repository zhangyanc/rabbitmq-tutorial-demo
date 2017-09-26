package pers.zyc.rbq.demo;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhangyancheng
 */
public class RPCClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(RPCClient.class);

    private Connection connection;
    private Channel channel;
    private static final String REQUEST_QUEUE_NAME = "rpc_queue";
    private String replyQueueName;
    private ConcurrentMap<String, ResultFuture> resultFutureMap = new ConcurrentHashMap<>();

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connection = connectionFactory.newConnection();
        channel = connection.createChannel();
        replyQueueName = channel.queueDeclare().getQueue();

        channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                ResultFuture future = resultFutureMap.remove(properties.getCorrelationId());
                if (future != null) {
                    future.set(new String(body, "UTF-8"));
                }
            }
        });
    }

    public String call(String message) throws IOException, InterruptedException {
        final String corrId = UUID.randomUUID().toString();
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .correlationId(corrId).replyTo(replyQueueName).build();
        ResultFuture future = new ResultFuture();
        resultFutureMap.put(corrId, future);

        channel.basicPublish("", REQUEST_QUEUE_NAME, props, message.getBytes("UTF-8"));
        return future.get();
    }

    public void close() throws IOException {
        connection.close();
    }

    private class ResultFuture {
        private String result;
        private CountDownLatch cdl = new CountDownLatch(1);

        void set(String result) {
            this.result = result;
            this.cdl.countDown();
        }

        String get() throws InterruptedException {
            cdl.await();
            return result;
        }
    }

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        final RPCClient rpcClient = new RPCClient();

        ExecutorService threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                return thread;
            }
        });
        final AtomicInteger tasks = new AtomicInteger(30);
        final CountDownLatch cdl = new CountDownLatch(tasks.get());

        while (tasks.get() > 0) {
            threadPool.submit(new Runnable() {
                int n = tasks.getAndDecrement();
                @Override
                public void run() {
                    try {
                        String result = rpcClient.call(String.valueOf(n));
                        LOGGER.info("fib({}) = {}", n, result);
                    } catch (IOException | InterruptedException e) {
                        LOGGER.error("fib call error!", e);
                    } finally {
                        cdl.countDown();
                    }
                }
            });
        }
        cdl.await();
        rpcClient.close();
    }
}
