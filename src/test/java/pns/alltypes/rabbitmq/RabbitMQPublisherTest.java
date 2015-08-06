/**
 *
 */
package pns.alltypes.rabbitmq;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pns.alltypes.rabbitmq.config.RabbitConnectionConfig;
import pns.alltypes.rabbitmq.io.AmqpChannel;
import pns.alltypes.rabbitmq.sustained.RabbitMQConnectionManager;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * @author arung
 */
public class RabbitMQPublisherTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQPublisherTest.class);
    private static final RabbitMQPublisherTest RABBIT_MQ_PUBLISHER_TEST = new RabbitMQPublisherTest();
    private static final ConnectionFactory CONN_FACTORY = new ConnectionFactory();

    private RabbitMQConnectionManager RABBIT_MQ_CONNECTION_MANAGER;

    private RabbitMQPublisherTest() {

    }

    public static void main(final String[] args) throws IOException {
        if (args.length < 4) {
            RabbitMQPublisherTest.LOGGER.error(String.format("Reqiured parameters missing. Use java -jar <jar file> user pass queueName Host"));
        }

        RabbitMQPublisherTest.RABBIT_MQ_PUBLISHER_TEST.testRabbitMQ(args[0], args[1], args[2], args[3]);
    }

    /**
     * @param password
     * @param userName
     * @throws IOException
     */
    private void testRabbitMQ(final String userName, final String password, final String queueName, final String host) throws IOException {

        RabbitMQPublisherTest.CONN_FACTORY.setUsername(userName);
        RabbitMQPublisherTest.CONN_FACTORY.setPassword(password);
        RabbitMQPublisherTest.CONN_FACTORY.setHost(host);
        RabbitMQPublisherTest.CONN_FACTORY.setRequestedHeartbeat(25);
        RABBIT_MQ_CONNECTION_MANAGER = RabbitMQConnectionManager.getInstance(new RabbitConnectionConfig(RabbitMQPublisherTest.CONN_FACTORY, new Address[] {}));
        RABBIT_MQ_CONNECTION_MANAGER.hintResourceAddition();
        final AmqpChannel channel = RABBIT_MQ_CONNECTION_MANAGER.getChannel();
        final Channel ioChannel = channel.getChannel();
        ioChannel.basicConsume(queueName, false, new DefaultConsumer(ioChannel) {
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
