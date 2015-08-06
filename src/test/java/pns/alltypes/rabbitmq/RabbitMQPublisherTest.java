/**
 *
 */
package pns.alltypes.rabbitmq;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import pns.alltypes.rabbitmq.config.RabbitConnectionConfig;
import pns.alltypes.rabbitmq.io.AmqpChannel;
import pns.alltypes.rabbitmq.sustained.RabbitMQConnectionManager;

/**
 * @author arung
 */
public class RabbitMQPublisherTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQPublisherTest.class);
    private static final ConnectionFactory CONN_FACTORY = new ConnectionFactory();

    private static String userName;
    private static String password;
    private static String queueName;
    private static String host;

    @BeforeClass
    public static void init() {
        RabbitMQPublisherTest.userName = System.getProperty("userName");
        RabbitMQPublisherTest.password = System.getProperty("password");
        RabbitMQPublisherTest.queueName = System.getProperty("queueName");
        RabbitMQPublisherTest.host = System.getProperty("host");

    }

    private RabbitMQConnectionManager RABBIT_MQ_CONNECTION_MANAGER;

    /**
     * @param password
     * @param userName
     * @throws IOException
     */
    @Test
    public void testRabbitMQConsumption() throws IOException {

        RabbitMQPublisherTest.CONN_FACTORY.setUsername(RabbitMQPublisherTest.userName);
        RabbitMQPublisherTest.CONN_FACTORY.setPassword(RabbitMQPublisherTest.password);
        RabbitMQPublisherTest.CONN_FACTORY.setHost(RabbitMQPublisherTest.host);
        RabbitMQPublisherTest.CONN_FACTORY.setRequestedHeartbeat(25);
        RABBIT_MQ_CONNECTION_MANAGER = RabbitMQConnectionManager.getInstance(new RabbitConnectionConfig(RabbitMQPublisherTest.CONN_FACTORY, new Address[] {}));
        RABBIT_MQ_CONNECTION_MANAGER.hintResourceAddition();
        final AmqpChannel channel = RABBIT_MQ_CONNECTION_MANAGER.getChannel();
        final Channel ioChannel = channel.getChannel();
        ioChannel.basicConsume(RabbitMQPublisherTest.queueName, false, new DefaultConsumer(ioChannel) {
            /* (non-Javadoc)
             * @see com.rabbitmq.client.DefaultConsumer#handleDelivery(java.lang.String, com.rabbitmq.client.Envelope, com.rabbitmq.client.AMQP.BasicProperties, byte[])
             */
            @Override
            public void handleDelivery(final String consumerTag, final Envelope envelope, final BasicProperties properties, final byte[] body)
                    throws IOException {
                RabbitMQPublisherTest.LOGGER.info(String.format("Got message %s", new String(body)));
            }
        });

    }
}
