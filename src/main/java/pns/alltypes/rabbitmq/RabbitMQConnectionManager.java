package pns.alltypes.rabbitmq;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pns.alltypes.tasks.DelayedTaskQueue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * @author arung
 */
public class RabbitMQConnectionManager implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = -7129244898371494140L;
    /**
     *
     */
    private static final int MAX_CHANNELS_PER_CONSUMER = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConnectionManager.class);

    static class AmqpConnection {

        private static final Lock connectionOperations = new ReentrantLock();
        private static final Lock channelOperationsLock = new ReentrantLock();

        private static final BlockingDeque<AmqpConnection> CONN_LIST = new LinkedBlockingDeque<AmqpConnection>();
        private static final Map<String, Integer> CONNECTION_TO_CHANNEL_COUNTER = new ConcurrentHashMap<String, Integer>();
        private static final Map<String, List<AmqpChannel>> CONNECTION_TO_CHANNEL_MAP = new ConcurrentHashMap<String, List<AmqpChannel>>();
        private static final Map<String, AmqpConnection> CONNECTIONID_TO_CONNECTIONMAP = new ConcurrentHashMap<String, AmqpConnection>();

        private final AtomicBoolean markedForDeletion = new AtomicBoolean(Boolean.FALSE);

        private String connectionId;

        private Connection connection;

        public AmqpConnection(final String connectionId, final Connection conn) {
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

            return AmqpConnection.CONNECTIONID_TO_CONNECTIONMAP.get(connectionId);

        }

        /**
         * @param connectionId2
         */
        public void createConnectionMetaData(final String connectionId2) {
            try {

                // lock
                AmqpConnection.connectionOperations.lock();

                AmqpConnection.CONNECTION_TO_CHANNEL_COUNTER.put(connectionId, 0);
                AmqpConnection.CONNECTIONID_TO_CONNECTIONMAP.put(connectionId, this);
                try {
                    AmqpConnection.CONN_LIST.putFirst(this);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                AmqpConnection.CONNECTION_TO_CHANNEL_MAP.put(connectionId, new CopyOnWriteArrayList<AmqpChannel>());
            } finally {

                // unlock
                AmqpConnection.connectionOperations.unlock();
            }
        }

        /**
         * @param connectionId2
         */
        public void removeMetaInfo(final String connectionId2) {

            // Remove connections from connection id references
            AmqpConnection.CONNECTIONID_TO_CONNECTIONMAP.remove(connectionId);
            AmqpConnection.CONNECTION_TO_CHANNEL_COUNTER.remove(connectionId);
            markedForDeletion.getAndSet(Boolean.TRUE);

        }

        public void createConnectionAndUpdateMeta(final AmqpChannel amqpChannel) {

            AmqpConnection.CONNECTION_TO_CHANNEL_COUNTER.put(this.getConnectionId(),
                    AmqpConnection.CONNECTION_TO_CHANNEL_COUNTER.get(this.getConnectionId()) + 1);
            final List<AmqpChannel> channels = AmqpConnection.CONNECTION_TO_CHANNEL_MAP.get(this.getConnectionId());
            channels.add(amqpChannel);
            AmqpConnection.CONNECTION_TO_CHANNEL_MAP.put(this.getConnectionId(), channels);

        }

        private void shutdownConnectionsAndRemoveMeta(final String connectionId) {
            try {
                // lock
                AmqpConnection.connectionOperations.lock();

                removeMetaInfo(connectionId);

                final List<RabbitMQConnectionManager.AmqpChannel> channels = AmqpConnection.CONNECTION_TO_CHANNEL_MAP.get(connectionId);
                for (final RabbitMQConnectionManager.AmqpChannel channel : channels) {
                    shutdownChannel(channel);
                }
            } finally {

                AmqpConnection.CONNECTION_TO_CHANNEL_MAP.remove(connectionId);
                AmqpConnection.CONN_LIST.remove(this);
                // unlock
                AmqpConnection.connectionOperations.unlock();
            }
        }

        private void shutdownChannel(final RabbitMQConnectionManager.AmqpChannel channel) {
            if (RabbitMQConnectionManager.LOGGER.isTraceEnabled()) {
                RabbitMQConnectionManager.LOGGER.trace("Shutting down channel " + channel);
            }
            try {
                channel.markForDeletion();
                RabbitMQConnectionManager.CHANNEL_VIEW.removeChannel(channel);
                // RabbitMQQueuePublisher.CHANNEL_LIST.remove(channel);
                channel.getChannel().close();
            } catch (final IOException e) {
                RabbitMQConnectionManager.LOGGER.error(String.format("Error occured in shutting down %s", e));
                // Thread.currentThread().interrupt();
                // ignore;
            } catch (final ShutdownSignalException e) {
                RabbitMQConnectionManager.LOGGER.error(String.format("Error occured in shutting down %s", e));
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

    }

    public static class AmqpChannel {
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

    }

    static class ChannelView {

        private static final BlockingDeque<AmqpChannel> CHANNEL_LIST = new LinkedBlockingDeque<AmqpChannel>();
        private static final ChannelView CHANNEL_VIEW = new ChannelView();

        public AmqpChannel getChannel() {
            AmqpChannel channel = null;
            try {
                RabbitMQConnectionManager.LOGGER.trace("ChannelView: getting channel");
                channel = ChannelView.CHANNEL_LIST.takeFirst();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();

            }

            return channel;

        }

        public void releaseChannel(final AmqpChannel channel) {
            try {
                if (!channel.isMarkForDeletion()) {
                    ChannelView.CHANNEL_LIST.putFirst(channel);
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public void removeChannel(final AmqpChannel channel) {
            ChannelView.CHANNEL_LIST.remove(channel);
        }

        public static ChannelView getInstance() {
            return ChannelView.CHANNEL_VIEW;
        }

        public void addChannel(final AmqpChannel amqpChannel) {
            ChannelView.CHANNEL_LIST.add(amqpChannel);
        }

    }

    private static final DelayedTaskQueue DELAYED_TASK_QUEUE_CONN = new DelayedTaskQueue(1, "ConnectionCreator", "ConnectionPool", 2000);
    private static final DelayedTaskQueue DELAYED_TASK_QUEUE_CHANNEL = new DelayedTaskQueue(1, "ChannelCreator", "ChannelPool", 2000);

    private static final ChannelView CHANNEL_VIEW = ChannelView.getInstance();

    // private static final BlockingQueue<AmqpConnection> CONN_LIST = new LinkedBlockingQueue<AmqpConnection>();
    // private static final BlockingQueue<AmqpChannel> CHANNEL_LIST = new LinkedBlockingQueue<AmqpChannel>();
    // private static final Map<String, List<AmqpChannel>> CONNECTION_TO_CHANNEL_MAP = new ConcurrentHashMap<String,
    // List<AmqpChannel>>();

    private volatile static boolean inited = false;
    private static RabbitConnectionConfig CONN_CONFIG;

    private static final RabbitMQConnectionManager RABBITMQ_CONNECTION_MANAGER = new RabbitMQConnectionManager();

    private RabbitMQConnectionManager() {

    }

    public static synchronized RabbitMQConnectionManager getInstance(final RabbitConnectionConfig connectionConfig) {

        if (!RabbitMQConnectionManager.inited) {
            RabbitMQConnectionManager.CONN_CONFIG = connectionConfig;
            RabbitMQConnectionManager.inited = true;
            // RabbitMQConnectionManager.createResources();
        }

        return RabbitMQConnectionManager.RABBITMQ_CONNECTION_MANAGER;

    }

    //
    // private static void createResources() {
    // RabbitMQConnectionManager.createConnectionList();
    //
    // RabbitMQConnectionManager.createChannelList();
    // }

    public void destroy() {

    }

    public void addConnection() {
        // every connection create a backup.
        RabbitMQConnectionManager.createConnection();
        RabbitMQConnectionManager.createConnection();

    }

    public void addChannel() {
        // every channel create a backup
        RabbitMQConnectionManager.createChannel();
    }

    private static void createChannel() {
        RabbitMQConnectionManager.DELAYED_TASK_QUEUE_CHANNEL.addTask(new ChannelCreator());
    }

    private static final class ChannelCreator implements Runnable {

        @Override
        public void run() {

            AmqpConnection conn = null;
            try {

                // lock
                AmqpConnection.channelOperationsLock.lock();
                if (RabbitMQConnectionManager.LOGGER.isTraceEnabled()) {
                    RabbitMQConnectionManager.LOGGER.trace("Tring to acquire connection");

                }
                conn = RabbitMQConnectionManager.selectConnection();
                if (RabbitMQConnectionManager.LOGGER.isTraceEnabled()) {
                    RabbitMQConnectionManager.LOGGER.trace(String.format("selected connection %s for creating channel", conn));

                }
                AmqpChannel amqpChannel = null;
                if (conn != null) {
                    if (RabbitMQConnectionManager.LOGGER.isTraceEnabled()) {
                        RabbitMQConnectionManager.LOGGER.trace(String.format("Acquired connection: %s", conn));
                    }
                    if (RabbitMQConnectionManager.LOGGER.isTraceEnabled()) {
                        RabbitMQConnectionManager.LOGGER.trace("Trying to acquire channel");
                    }
                    final Channel channel = conn.getConnection().createChannel();
                    // channel.addShutdownListener(new ShutdownListener() {
                    //
                    // @Override
                    // public void shutdownCompleted(final ShutdownSignalException cause) {
                    // // recreate yourself
                    // RabbitMQConnectionManager.createChannel();
                    // }
                    // });
                    amqpChannel = new AmqpChannel(UUID.randomUUID().toString(), channel);
                    if (RabbitMQConnectionManager.LOGGER.isTraceEnabled()) {
                        RabbitMQConnectionManager.LOGGER.trace(String.format("Acquired channel: %s", amqpChannel.getChannel()));
                    }
                    // RabbitMQQueuePublisher.CHANNEL_LIST.add(amqpChannel);
                    RabbitMQConnectionManager.CHANNEL_VIEW.addChannel(amqpChannel);
                    conn.createConnectionAndUpdateMeta(amqpChannel);
                }

                // try {
                // channelLatch.await();
                // } catch (final InterruptedException e) {
                // Thread.currentThread().interrupt();
                // }

            } catch (final Exception e) {
                if (e instanceof IOException) {
                    RabbitMQConnectionManager.createChannel();
                }
                RabbitMQConnectionManager.LOGGER.error(String.format("Catch any spurious exceptions %s", e));
            } finally {
                AmqpConnection.channelOperationsLock.unlock();
            }

        }

    }

    private static AmqpConnection selectConnection() {
        final Set<String> keyString = AmqpConnection.CONNECTION_TO_CHANNEL_COUNTER.keySet();
        AmqpConnection conn = null;
        while (conn == null) {
            // boolean firstTime = true;
            // int lowestCount = 1;
            for (final String key : keyString) {
                // take only non marked for deletion ones

                final AmqpConnection tempConn = AmqpConnection.CONNECTIONID_TO_CONNECTIONMAP.get(key);
                if (tempConn != null && !tempConn.isConnectionClosed()) {
                    final Integer channelCntInteger = AmqpConnection.CONNECTION_TO_CHANNEL_COUNTER.get(key);
                    if (channelCntInteger != null) {
                        // find one with lowest channels
                        final int channelCount = channelCntInteger;// Avoid NP
                        // find the lowest
                        // lowestCount: 0
                        // Channel Counts: 4,0,2,3 == 0
                        if (channelCount == RabbitMQConnectionManager.MAX_CHANNELS_PER_CONSUMER) {// assume one channel
                            // per connection --
                            // best for rabbitmq
                            conn = tempConn;
                            break;
                        }
                        // find the lowest
                        // lowestCount: 1
                        // lowestCount: 1
                        // Channel Counts: 1,4,2,3 == 1
                        // Channel Counts: 4,3,2,3 == 1
                        // else {
                        // if (firstTime) {
                        // lowestCount = channelCount;
                        // firstTime = false;
                        // }
                        // if (lowestCount > channelCount) {
                        // lowestCount = channelCount;
                        // conn = tempConn;
                        // }
                        //
                        // }

                    }
                }
            }
        }

        if (RabbitMQConnectionManager.LOGGER.isTraceEnabled()) {
            RabbitMQConnectionManager.LOGGER.trace("selectConnection conn:" + conn);
        }
        return conn;
    }

    private static void createConnection() {

        RabbitMQConnectionManager.DELAYED_TASK_QUEUE_CONN.addTask(new ConnectionCreator());

    }

    private static final class ConnectionCreator implements Runnable {

        @Override
        public void run() {

            try {
                establishConnection();
            } catch (final Exception e) {
                if (e instanceof IOException) {
                    RabbitMQConnectionManager.createConnection();
                }
                RabbitMQConnectionManager.LOGGER.error(String.format("Catch any spurious exceptions %s", e));
            }

        }

        private void establishConnection() throws IOException {
            if (RabbitMQConnectionManager.LOGGER.isTraceEnabled()) {
                RabbitMQConnectionManager.LOGGER.trace("inside establishConnection");
            }
            AmqpConnection amqpConnection = null;

            if (RabbitMQConnectionManager.LOGGER.isTraceEnabled()) {
                RabbitMQConnectionManager.LOGGER.trace("Trying to create connection");
            }
            final ConnectionFactory connectionFactory = RabbitMQConnectionManager.CONN_CONFIG.getConnectionFactory();
            final Connection connection = RabbitMQConnectionManager.CONN_CONFIG.getHighAvailabilityHosts().length == 0 ? connectionFactory.newConnection()
                    : connectionFactory.newConnection(RabbitMQConnectionManager.CONN_CONFIG.getHighAvailabilityHosts());
            final String connectionId = UUID.randomUUID().toString();
            amqpConnection = new AmqpConnection(connectionId, connection);
            final AmqpConnection amqpConnection2 = amqpConnection;
            amqpConnection2.createConnectionMetaData(connectionId);

            connection.addShutdownListener(new ShutdownListener() {

                @Override
                public void shutdownCompleted(final ShutdownSignalException cause) {
                    RabbitMQConnectionManager.LOGGER.info("shutdownCompleted " + amqpConnection2);
                    if (RabbitMQConnectionManager.LOGGER.isTraceEnabled()) {
                        RabbitMQConnectionManager.LOGGER.trace("shutdownCompleted for connection " + amqpConnection2);
                    }

                    try {

                        amqpConnection2.shutdownConnectionsAndRemoveMeta(connectionId);
                        RabbitMQConnectionManager.createConnection();

                    } catch (final Exception e) {
                        RabbitMQConnectionManager.LOGGER.error(String.format("Error occured on shutdown is %s", e));

                    } finally {
                        // dont care for other exception . go and create next connection and channel
                    }

                }

            });
            // break;
            if (RabbitMQConnectionManager.LOGGER.isTraceEnabled()) {
                RabbitMQConnectionManager.LOGGER.trace(String.format("Created connection: %s", amqpConnection.getConnection()));
            }

        }

    }

    public AmqpChannel getChannel() {

        final AmqpChannel amqpChannel = RabbitMQConnectionManager.CHANNEL_VIEW.getChannel();
        return amqpChannel;
    }

    /**
     *
     */
    public void hintResourceAddition() {
        RabbitMQConnectionManager.createConnection();
        RabbitMQConnectionManager.createChannel();

    }

}
