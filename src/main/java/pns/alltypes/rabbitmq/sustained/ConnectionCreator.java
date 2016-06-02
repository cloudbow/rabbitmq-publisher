package pns.alltypes.rabbitmq.sustained;

import java.io.IOException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pns.alltypes.rabbitmq.io.AmqpConnection;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

public class ConnectionCreator implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionCreator.class);

    private final RabbitMQConnectionManager rabbitMQConnectionManager;

    public ConnectionCreator(final RabbitMQConnectionManager rabbitMQConnectionManager) {
        this.rabbitMQConnectionManager = rabbitMQConnectionManager;

    }

    @Override
    public void run() {

        try {
            establishConnection();
        } catch (final Exception e) {
            if (e instanceof IOException) {
                rabbitMQConnectionManager.createConnection();
            }
            ConnectionCreator.LOGGER.error(String.format("Catch any spurious exceptions %s", e));
        }

    }

    private void establishConnection() throws IOException {
        if (ConnectionCreator.LOGGER.isTraceEnabled()) {
            ConnectionCreator.LOGGER.trace("inside establishConnection");
        }
        AmqpConnection amqpConnection = null;

        if (ConnectionCreator.LOGGER.isTraceEnabled()) {
            ConnectionCreator.LOGGER.trace("Trying to create connection");
        }
        final ConnectionFactory connectionFactory = rabbitMQConnectionManager.getConnectionConfig().getConnectionFactory();
        final Connection connection = rabbitMQConnectionManager.getConnectionConfig().getHighAvailabilityHosts().length == 0 ? connectionFactory
                .newConnection() : connectionFactory.newConnection(rabbitMQConnectionManager.getConnectionConfig().getHighAvailabilityHosts());
                final String connectionId = UUID.randomUUID().toString();
                amqpConnection = new AmqpConnection(connectionId, connection, rabbitMQConnectionManager);
                final AmqpConnection amqpConnection2 = amqpConnection;
                amqpConnection2.createConnectionMetaData(connectionId);

                connection.addShutdownListener(new ShutdownListener() {

                    @Override
                    public void shutdownCompleted(final ShutdownSignalException cause) {
                        ConnectionCreator.LOGGER.info("shutdownCompleted " + amqpConnection2);
                        if (ConnectionCreator.LOGGER.isTraceEnabled()) {
                            ConnectionCreator.LOGGER.trace("shutdownCompleted for connection " + amqpConnection2);
                        }

                        try {

                            amqpConnection2.shutdownConnectionsAndRemoveMeta(connectionId);

                        } catch (final Exception e) {
                            ConnectionCreator.LOGGER.error(String.format("Error occured on shutdown is %s", e));

                        } finally {
                            // dont care for other exception . go and create next connection and channel
                            rabbitMQConnectionManager.createConnection();
                        }

                    }

                });
                // break;
                if (ConnectionCreator.LOGGER.isTraceEnabled()) {
                    ConnectionCreator.LOGGER.trace(String.format("Created connection: %s", amqpConnection.getConnection()));
                }

    }

}