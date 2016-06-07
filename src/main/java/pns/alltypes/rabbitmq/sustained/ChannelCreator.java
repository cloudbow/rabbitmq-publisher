package pns.alltypes.rabbitmq.sustained;

import java.io.IOException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

import pns.alltypes.rabbitmq.io.AmqpChannel;
import pns.alltypes.rabbitmq.io.AmqpConnection;

public class ChannelCreator implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelCreator.class);

    private final RabbitMQConnectionManager rabbitMQConnectionManager;

    public ChannelCreator(final RabbitMQConnectionManager rabbitMQConnectionManager) {
        this.rabbitMQConnectionManager = rabbitMQConnectionManager;

    }

    @Override
    public void run() {

        AmqpConnection conn = null;
        try {

            // lock
            rabbitMQConnectionManager.getChannelOperationsLock().lock();
            if (ChannelCreator.LOGGER.isTraceEnabled()) {
                ChannelCreator.LOGGER.trace("Tring to acquire connection");

            }
            conn = rabbitMQConnectionManager.selectConnection();
            final AmqpConnection conn2 = conn;
            if (ChannelCreator.LOGGER.isTraceEnabled()) {
                ChannelCreator.LOGGER.trace(String.format("selected connection %s for creating channel", conn));

            }
            AmqpChannel amqpChannel = null;
            if (conn != null) {
                if (ChannelCreator.LOGGER.isTraceEnabled()) {
                    ChannelCreator.LOGGER.trace(String.format("Acquired connection: %s", conn));
                }
                if (ChannelCreator.LOGGER.isTraceEnabled()) {
                    ChannelCreator.LOGGER.trace("Trying to acquire channel");
                }
                final Channel channel = conn.getConnection().createChannel();
                channel.addShutdownListener(new ShutdownListener() {

                    @Override
                    public void shutdownCompleted(final ShutdownSignalException cause) {
                       if(LOGGER.isTraceEnabled()) {
                          LOGGER.trace(String.format("SHUTDOWN SIGNAL got for channel %s"),channel);
                       }
                        // recreate yourself
                        rabbitMQConnectionManager.hintResourceAddition();
                        if (conn2 != null) {
                            if(LOGGER.isTraceEnabled()) {
                               LOGGER.trace(String.format("Shutting down connection %s of channel %s", conn2, channel),channel);
                            }
                            conn2.close();
                        }
                    }
                });
                amqpChannel = new AmqpChannel(UUID.randomUUID().toString(), channel);
                if (ChannelCreator.LOGGER.isTraceEnabled()) {
                    ChannelCreator.LOGGER.trace(String.format("Acquired amqp %s , channel: %s",amqpChannel, amqpChannel.getChannel()));
                }
                // RabbitMQQueuePublisher.CHANNEL_LIST.add(amqpChannel);
                rabbitMQConnectionManager.getChannelView().addChannel(amqpChannel);
                conn.createConnectionAndUpdateMeta(amqpChannel);
            }

            // try {
            // channelLatch.await();
            // } catch (final InterruptedException e) {
            // Thread.currentThread().interrupt();
            // }

        } catch (final Exception e) {
            if (e instanceof IOException) {
                rabbitMQConnectionManager.createChannel();
            }
            ChannelCreator.LOGGER.error(String.format("Catch any spurious exceptions %s", e));
        } finally {
            rabbitMQConnectionManager.getChannelOperationsLock().unlock();
        }

    }

}