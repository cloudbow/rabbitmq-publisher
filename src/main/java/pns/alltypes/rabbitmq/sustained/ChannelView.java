/**
 *
 */
package pns.alltypes.rabbitmq.sustained;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pns.alltypes.rabbitmq.io.AmqpChannel;

public class ChannelView {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelView.class);
    private final RabbitMQConnectionManager rabbitMQConnectionManager;

    /**
     * @param rabbitMQConnectionManager
     */
    public ChannelView(final RabbitMQConnectionManager rabbitMQConnectionManager) {
        this.rabbitMQConnectionManager = rabbitMQConnectionManager;
    }

    public AmqpChannel getChannel() {
        AmqpChannel channel = null;
        try {
            ChannelView.LOGGER.trace("ChannelView: getting channel");
            channel = rabbitMQConnectionManager.getChannelList().takeFirst();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();

        }

        return channel;

    }

    public void releaseChannel(final AmqpChannel channel) {
        try {
            if (!channel.isMarkForDeletion()) {
                rabbitMQConnectionManager.getChannelList().putFirst(channel);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void removeChannel(final AmqpChannel channel) {
        rabbitMQConnectionManager.getChannelList().remove(channel);
    }

    public void addChannel(final AmqpChannel amqpChannel) {
        rabbitMQConnectionManager.getChannelList().add(amqpChannel);
    }

}