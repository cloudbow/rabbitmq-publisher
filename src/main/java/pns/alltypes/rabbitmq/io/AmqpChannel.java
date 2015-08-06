/**
 *
 */
package pns.alltypes.rabbitmq.io;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;

public class AmqpChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpChannel.class);
    private Channel channel;
    private final AtomicBoolean deleted = new AtomicBoolean(false);

    public AmqpChannel(final String channelId, final Channel channel) {
        this.setChannel(channel);
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(final Channel channel) {
        this.channel = channel;
    }

    public void markForDeletion() {
        deleted.getAndSet(Boolean.TRUE);

    }

    public boolean isMarkForDeletion() {
        return deleted.get();
    }

    /**
     *
     */
    public void release() {

    }

    /**
     *
     */
    public void close() {
        try {
            channel.close();
        } catch (final AlreadyClosedException e) {
            AmqpChannel.LOGGER.error(String.format("Rabbit Connection already closed %s", e));
        } catch (final IOException e) {
            AmqpChannel.LOGGER.error(String.format("IO Exception occured on close %s", e));
        }
    }

}