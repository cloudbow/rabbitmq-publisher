package pns.alltypes.rabbitmq.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitConnectionConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitConnectionConfig.class);
    private ConnectionFactory connectionFactory;
    private Address[] highAvailabilityHosts = new Address[] {};
    private volatile boolean inited = false;

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

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {
        boolean equals = false;

        if (this == obj) {
            equals = true;
        } else {

            if (obj == null || !(obj instanceof RabbitConnectionConfig)) {
                return false;
            } else {

                final RabbitConnectionConfig castedObj = (RabbitConnectionConfig) obj;

                if (this.connectionFactory == null || castedObj.getConnectionFactory() == null) {
                    equals = false;
                } else {
                    equals = this.connectionFactory.getHost().equalsIgnoreCase(castedObj.getConnectionFactory().getHost());
                }

            }

        }

        return equals;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {

        return this.connectionFactory.getHost().hashCode();
    }

    public boolean isInited() {
        return inited;
    }

    public void setInited(final boolean inited) {
        this.inited = inited;
    }

}