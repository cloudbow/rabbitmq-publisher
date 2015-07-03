package pns.alltypes.rabbitmq;

import org.apache.log4j.Logger;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitConnectionConfig {

    private static final Logger LOGGER = Logger.getLogger(RabbitConnectionConfig.class);
    private ConnectionFactory connectionFactory;
    private Address[] highAvailabilityHosts;

    public RabbitConnectionConfig(final ConnectionFactory connectionFactory, final Address[] highAvailabilityHosts) {
        this.connectionFactory = connectionFactory;
        this.highAvailabilityHosts = highAvailabilityHosts;

    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(final ConnectionFactory connectionFactory) {
        if (RabbitConnectionConfig.LOGGER.isTraceEnabled()) {
            RabbitConnectionConfig.LOGGER.trace(String.format("Setting connection factory to %s", connectionFactory));
        }
        this.connectionFactory = connectionFactory;
    }

    public Address[] getHighAvailabilityHosts() {
        return highAvailabilityHosts;
    }

    public void setHighAvailabilityHosts(final Address[] highAvailabilityHosts) {
        if (RabbitConnectionConfig.LOGGER.isTraceEnabled()) {
            RabbitConnectionConfig.LOGGER.trace(String.format("Setting highAvailabilityHosts to %s", highAvailabilityHosts.toString()));
        }
        this.highAvailabilityHosts = highAvailabilityHosts;
    }

}