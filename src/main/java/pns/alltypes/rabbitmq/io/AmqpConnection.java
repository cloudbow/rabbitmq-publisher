/**
 *
 */
package pns.alltypes.rabbitmq.io;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;

import pns.alltypes.rabbitmq.sustained.RabbitMQConnectionManager;

public class AmqpConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConnection.class);

    private final AtomicBoolean markedForDeletion = new AtomicBoolean(Boolean.FALSE);

    private String connectionId;

    private Connection connection;

    private final RabbitMQConnectionManager rabbitMQConnectionManager;

    public AmqpConnection(final String connectionId, final Connection conn, final RabbitMQConnectionManager rabbitMQConnectionManager) {
        this.rabbitMQConnectionManager = rabbitMQConnectionManager;
        this.setConnectionId(connectionId);
        setConnection(conn);
    }

    public String getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(final String connectionId) {
        this.connectionId = connectionId;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(final Connection connection) {
        this.connection = connection;
    }

    public AmqpConnection getConnectionForConnId(final String connectionId) {

        return rabbitMQConnectionManager.getConnectionIdToConnMap().get(connectionId);

    }

    /**
     * @param connectionId2
     */
    public void createConnectionMetaData(final String connectionId2) {
        try {

            // lock
            rabbitMQConnectionManager.getConnOperationLock().lock();

            rabbitMQConnectionManager.getConnectionToChannelCounter().put(connectionId, 0);
            rabbitMQConnectionManager.getConnectionIdToConnMap().put(connectionId, this);
            try {
                rabbitMQConnectionManager.getConnList().putFirst(this);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            rabbitMQConnectionManager.getConnectionToChannelMap().put(connectionId, new CopyOnWriteArrayList<AmqpChannel>());
        } finally {

            // unlock
            rabbitMQConnectionManager.getConnOperationLock().unlock();
        }
    }

    /**
     * @param connectionId2
     */
    public void removeMetaInfo(final String connectionId2) {

        // Remove connections from connection id references
        rabbitMQConnectionManager.getConnectionIdToConnMap().remove(connectionId);
        rabbitMQConnectionManager.getConnectionToChannelCounter().remove(connectionId);
        markedForDeletion.getAndSet(Boolean.TRUE);

    }

    public void createConnectionAndUpdateMeta(final AmqpChannel amqpChannel) {

        rabbitMQConnectionManager.getConnectionToChannelCounter().put(this.getConnectionId(),
                rabbitMQConnectionManager.getConnectionToChannelCounter().get(this.getConnectionId()) + 1);
        final List<AmqpChannel> channels = rabbitMQConnectionManager.getConnectionToChannelMap().get(this.getConnectionId());
        channels.add(amqpChannel);
        rabbitMQConnectionManager.getConnectionToChannelMap().put(this.getConnectionId(), channels);

    }

    public void shutdownConnectionsAndRemoveMeta(final String connectionId) {
        try {
            // lock
            rabbitMQConnectionManager.getConnOperationLock().lock();

            removeMetaInfo(connectionId);

            final List<AmqpChannel> channels = rabbitMQConnectionManager.getConnectionToChannelMap().get(connectionId);
            for (final AmqpChannel channel : channels) {
                shutdownChannel(channel);
            }
        } finally {

            rabbitMQConnectionManager.getConnectionToChannelMap().remove(connectionId);
            rabbitMQConnectionManager.getConnList().remove(this);
            // unlock
            rabbitMQConnectionManager.getConnOperationLock().unlock();
        }
    }

    private void shutdownChannel(final AmqpChannel channel) {
        if (AmqpConnection.LOGGER.isTraceEnabled()) {
            AmqpConnection.LOGGER.trace(String.format("Shutting down channel %s " , channel));
        }
        try {
            channel.markForDeletion();
            rabbitMQConnectionManager.getChannelView().removeChannel(channel);
            // RabbitMQQueuePublisher.CHANNEL_LIST.remove(channel);
            channel.getChannel().close();
        } catch (final IOException e) {
            AmqpConnection.LOGGER.error(String.format("Error occured in shutting down channel %s", e));
            // Thread.currentThread().interrupt();
            // ignore;
        } catch (final ShutdownSignalException e) {
            AmqpConnection.LOGGER.error(String.format("Error occured in shutting down channel %s", e));
            // This exception thrown if channel is already closed (by connection shutdown)
            // ignore
        } finally {
            //

        }
    }

    /**
     * @return
     */
    public boolean isConnectionClosed() {
        return markedForDeletion.get();
    }

    /**
     *
     */
    public void close() {
        shutdownConnectionsAndRemoveMeta(connectionId);

    }

}