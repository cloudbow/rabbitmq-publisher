/**
 *
 */
package pns.alltypes.rabbitmq.test;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

import pns.alltypes.rabbitmq.config.RabbitConnectionConfig;
import pns.alltypes.rabbitmq.io.AmqpChannel;
import pns.alltypes.rabbitmq.sustained.RabbitMQConnectionManager;

/**
 * @author arung
 */
public class RabbitTester {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitTester.class);

    private static final ConnectionFactory CONN_FACTORY = new ConnectionFactory();

    private static RabbitMQConnectionManager RABBIT_MQ_CONNECTION_MANAGER;

    public static void main(final String[] args) throws IOException {

        try {
            final String userName = args[0];
            RabbitTester.CONN_FACTORY.setUsername(userName);
            final String password = args[1];
            RabbitTester.CONN_FACTORY.setPassword(password);
            final String host = args[2];
            RabbitTester.CONN_FACTORY.setHost(host);
            RabbitTester.CONN_FACTORY.setRequestedHeartbeat(25);
            RabbitTester.RABBIT_MQ_CONNECTION_MANAGER = RabbitMQConnectionManager
                    .getInstance(new RabbitConnectionConfig(RabbitTester.CONN_FACTORY, new Address[] {}));
            RabbitTester.RABBIT_MQ_CONNECTION_MANAGER.hintResourceAddition();
            final AmqpChannel channel = RabbitTester.RABBIT_MQ_CONNECTION_MANAGER.getChannel();
            final Channel ioChannel = channel.getChannel();
            final String message = args[3];
            final String queueName = args[4];
            String exchangeName = "";
            if (args.length > 5) {
                exchangeName = args[5];
            }
            ioChannel.basicPublish(exchangeName, queueName, null, message.getBytes());

        } catch (final IndexOutOfBoundsException e) {
            RabbitTester.LOGGER
                    .error(String.format("Please supply all the arguments in format java -jar <jar file name> user pass host message queue exchange %s", e));
        }

    }

}
