package pns.alltypes.rabbitmq.sustained;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pns.alltypes.rabbitmq.config.RabbitConnectionConfig;
import pns.alltypes.rabbitmq.io.AmqpChannel;
import pns.alltypes.rabbitmq.io.AmqpConnection;
import pns.alltypes.tasks.DelayedTaskQueue;

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
	static final int MAX_CHANNELS_PER_CONSUMER = 0;
	static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConnectionManager.class);

	private static final DelayedTaskQueue DELAYED_TASK_QUEUE_CONN = new DelayedTaskQueue(1, "ConnectionCreator",
			"ConnectionPool", 2000);
	private static final DelayedTaskQueue DELAYED_TASK_QUEUE_CHANNEL = new DelayedTaskQueue(1, "ChannelCreator",
			"ChannelPool", 2000);

	private final BlockingDeque<AmqpConnection> connList = new LinkedBlockingDeque<AmqpConnection>();
	private final Map<String, Integer> connectionToChannelCounter = new ConcurrentHashMap<String, Integer>();
	private final Map<String, List<AmqpChannel>> connectionToChannelMap = new ConcurrentHashMap<String, List<AmqpChannel>>();
	private final Map<String, AmqpConnection> connectionIdToConnMap = new ConcurrentHashMap<String, AmqpConnection>();

	private final Lock connOperationLock = new ReentrantLock();
	private final Lock channelOperationsLock = new ReentrantLock();

	private final BlockingDeque<AmqpChannel> channelList = new LinkedBlockingDeque<AmqpChannel>();

	private final ChannelView channelView;

	// private static final BlockingQueue<AmqpConnection> CONN_LIST = new
	// LinkedBlockingQueue<AmqpConnection>();
	// private static final BlockingQueue<AmqpChannel> CHANNEL_LIST = new
	// LinkedBlockingQueue<AmqpChannel>();
	// private static final Map<String, List<AmqpChannel>>
	// CONNECTION_TO_CHANNEL_MAP = new ConcurrentHashMap<String,
	// List<AmqpChannel>>();

	private RabbitConnectionConfig connectionConfig;

	private static final Map<RabbitConnectionConfig, RabbitMQConnectionManager> RABBIT_CONN_MANAGER_TO_CONN_CONFIG = new HashMap<RabbitConnectionConfig, RabbitMQConnectionManager>();

	private RabbitMQConnectionManager(final RabbitConnectionConfig connectionConfig2) {
		channelView = new ChannelView(this);
		this.setConnectionConfig(connectionConfig2);
	}

	public synchronized static RabbitMQConnectionManager getInstance(final RabbitConnectionConfig connectionConfig) {

		RabbitMQConnectionManager rabbitMQConnectionManager = RabbitMQConnectionManager.RABBIT_CONN_MANAGER_TO_CONN_CONFIG
				.get(connectionConfig);
		if (rabbitMQConnectionManager != null) {
			return rabbitMQConnectionManager;
		} else {
			rabbitMQConnectionManager = new RabbitMQConnectionManager(connectionConfig);
			RabbitMQConnectionManager.RABBIT_CONN_MANAGER_TO_CONN_CONFIG.put(connectionConfig,
					rabbitMQConnectionManager);
			return rabbitMQConnectionManager;
		}

	}

	public AmqpConnection selectConnection() {
		final Set<String> keyString = getConnectionToChannelCounter().keySet();
		AmqpConnection conn = null;
		while (conn == null) {
			// boolean firstTime = true;
			// int lowestCount = 1;
			for (final String key : keyString) {
				// take only non marked for deletion ones

				final AmqpConnection tempConn = getConnectionIdToConnMap().get(key);
				if (tempConn != null && !tempConn.isConnectionClosed()) {
					final Integer channelCntInteger = getConnectionToChannelCounter().get(key);
					if (channelCntInteger != null) {
						// find one with lowest channels
						final int channelCount = channelCntInteger;// Avoid NP
						// find the lowest
						// lowestCount: 0
						// Channel Counts: 4,0,2,3 == 0
						if (channelCount == RabbitMQConnectionManager.MAX_CHANNELS_PER_CONSUMER) {// assume
																									// one
																									// channel
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

		return conn;
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
		createConnection();
		createConnection();

	}

	public void addChannel() {
		// every channel create a backup
		createChannel();
	}

	public void createChannel() {
		RabbitMQConnectionManager.DELAYED_TASK_QUEUE_CHANNEL.addTask(new ChannelCreator(this));
	}

	public void createConnection() {

		RabbitMQConnectionManager.DELAYED_TASK_QUEUE_CONN.addTask(new ConnectionCreator(this));

	}

	public AmqpChannel closeAndReopenChannel(final AmqpChannel channel) {
		if (channel != null) {
			channel.close();
		}
		return getChannel();
	}

	public AmqpChannel getChannel() {

		final AmqpChannel amqpChannel = getChannelView().getChannel();
		return amqpChannel;
	}

	public AmqpChannel getChannelWithTimeout() {

		final AmqpChannel amqpChannel = getChannelView().getChannelWithTimeout();
		return amqpChannel;
	}

	/**
	 *
	 */
	public void hintResourceAddition() {
		createConnection();
		createChannel();

	}

	public RabbitConnectionConfig getConnectionConfig() {
		return connectionConfig;
	}

	public void setConnectionConfig(final RabbitConnectionConfig connectionConfig) {
		this.connectionConfig = connectionConfig;
	}

	public BlockingDeque<AmqpConnection> getConnList() {
		return connList;
	}

	public Map<String, Integer> getConnectionToChannelCounter() {
		return connectionToChannelCounter;
	}

	public Map<String, List<AmqpChannel>> getConnectionToChannelMap() {
		return connectionToChannelMap;
	}

	public Map<String, AmqpConnection> getConnectionIdToConnMap() {
		return connectionIdToConnMap;
	}

	public Lock getConnOperationLock() {
		return connOperationLock;
	}

	public Lock getChannelOperationsLock() {
		return channelOperationsLock;
	}

	public BlockingDeque<AmqpChannel> getChannelList() {
		return channelList;
	}

	public ChannelView getChannelView() {
		return channelView;
	}

}
