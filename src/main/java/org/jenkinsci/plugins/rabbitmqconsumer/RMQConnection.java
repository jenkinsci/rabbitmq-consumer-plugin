package org.jenkinsci.plugins.rabbitmqconsumer;

import hudson.util.Secret;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.jenkinsci.plugins.rabbitmqconsumer.channels.AbstractRMQChannel;
import org.jenkinsci.plugins.rabbitmqconsumer.channels.ConsumeRMQChannel;
import org.jenkinsci.plugins.rabbitmqconsumer.channels.PublishRMQChannel;
import org.jenkinsci.plugins.rabbitmqconsumer.events.RMQConnectionEvent;
import org.jenkinsci.plugins.rabbitmqconsumer.listeners.RMQChannelListener;
import org.jenkinsci.plugins.rabbitmqconsumer.listeners.RMQConnectionListener;
import org.jenkinsci.plugins.rabbitmqconsumer.notifiers.RMQConnectionNotifier;
import org.jenkinsci.plugins.rabbitmqconsumer.watchdog.ConnectionMonitor;
import org.jenkinsci.plugins.rabbitmqconsumer.watchdog.ReconnectTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Handle class for RabbitMQ connection.
 *
 * @author rinrinne a.k.a. rin_ne
 */
public class RMQConnection implements ShutdownListener, RMQChannelListener, RMQConnectionNotifier {

    private static final int TIMEOUT_CONNECTION_MILLIS = 30000;

    private static final Logger LOGGER = LoggerFactory.getLogger(RMQConnection.class);

    private final String serviceUri;
    private final String userName;
    private final Secret userPassword;
    private final long watchdogPeriod;
    private final ConnectionFactory factory;
    private Connection connection = null;
    private final Collection<AbstractRMQChannel> rmqChannels = new CopyOnWriteArraySet<AbstractRMQChannel>();
    private final Collection<RMQConnectionListener> rmqConnectionListeners = new CopyOnWriteArraySet<RMQConnectionListener>();
    private volatile RMQState state = RMQState.DISCONNECTED;

    /**
     * Creates instance with specified parameter.
     *
     * @param serviceUri
     *            the URI for RabbitMQ service.
     * @param userName
     *            the name of user.
     * @param userPassword
     *            the password of user.
     */
    public RMQConnection(String serviceUri, String userName, Secret userPassword) {
        this(serviceUri, userName, userPassword, 60000);
    }

    /**
     * Creates instance with specified parameter.
     *
     * @param serviceUri
     *            the URI for RabbitMQ service.
     * @param userName
     *            the name of user.
     * @param userPassword
     *            the password of user.
     * @param watchdogPeriod
     *            the period of watchdog in seconds.
     */
    public RMQConnection(String serviceUri, String userName, Secret userPassword, long watchdogPeriod) {
        this.serviceUri = serviceUri;
        this.userName = userName;
        this.userPassword = userPassword;
        this.watchdogPeriod = watchdogPeriod;
        this.factory = new ConnectionFactory();
        this.factory.setConnectionTimeout(TIMEOUT_CONNECTION_MILLIS);
    }

    /**
     * Gets connection.
     *
     * @return the connection.
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Create pure channel.
     *
     * @return the channel.
     */
    public Channel createPureChannel() {
        Channel ch = null;
        if (connection != null && connection.isOpen()) {
            try {
                ch = connection.createChannel();
            } catch (Exception ex) {
                LOGGER.warn("Cannot create channel.");
            }
        }
        return ch;
    }

    /**
     * Gets URI for RabbitMQ service.
     *
     * @return the URI.
     */
    public String getServiceUri() {
        return serviceUri;
    }

    /**
     * Gets URI for RabbitMQ service.
     *
     * @return the URI.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Gets URI for RabbitMQ service.
     *
     * @return the URI.
     */
    public Secret getUserPassword() {
        return userPassword;
    }

    /**
     * Gets the list of RMQChannels.
     *
     * @return the collection of RMQChannels.
     */
    public Collection<AbstractRMQChannel> getRMQChannels() {
        return rmqChannels;
    }

    /**
     * Gets the list of ConsumeRMQChannels.
     *
     * @return the collection of ComsumeRMQChannels.
     */
    public Collection<ConsumeRMQChannel> getConsumeRMQChannels() {
        Collection<ConsumeRMQChannel> channels = new HashSet<ConsumeRMQChannel>();
        for (AbstractRMQChannel ch : rmqChannels) {
            if (ch instanceof ConsumeRMQChannel) {
                channels.add((ConsumeRMQChannel) ch);
            }
        }
        return channels;
    }

    /**
     * Gets the list of PublishRMQChannels.
     *
     * @return the list of PublishRMQChannels.
     */
    public Collection<PublishRMQChannel> getPublishRMQChannels() {
        Collection<PublishRMQChannel> channels = new HashSet<PublishRMQChannel>();
        for (AbstractRMQChannel ch : rmqChannels) {
            if (ch instanceof PublishRMQChannel) {
                channels.add((PublishRMQChannel) ch);
            }
        }
        return channels;
    }

    /**
     * Gets status of channel binds specified queue.
     *
     * @param queueName
     *            the queue name.
     * @return true if channel for specified queue is already established.
     */
    public boolean getConsumeChannelStatus(String queueName) {
        for (ConsumeRMQChannel ch : getConsumeRMQChannels()) {
            if (ch.getQueueName().equals(queueName)) {
                return ch.isConsumeStarted();
            }
        }
        return false;
    }

    /**
     * Open connection.
     *
     * @throws IOException
     *             thow if connection cannot be opend.
     */
    public void open() throws IOException {
        if (state == RMQState.DISCONNECTED) {
            try {
                factory.setUri(serviceUri);
                if (StringUtils.isNotEmpty(userName)) {
                    factory.setUsername(userName);
                }
                if (StringUtils.isNotEmpty(Secret.toString(userPassword))) {
                    factory.setPassword(Secret.toString(userPassword));
                }
                connection = factory.newConnection();
                connection.addShutdownListener(this);
                state = RMQState.CONNECTED;
                notifyOnOpen();
            } catch (Exception ex) {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Exception e) {
                        // nothing
                    } finally {
                        connection = null;
                    }
                }
                throw new IOException(ex);
            }
            ReconnectTimer timer = ReconnectTimer.get();
            if (timer != null) {
                timer.setRecurrencePeriod(watchdogPeriod);
            }
        } else {
            throw new IOException("Connection is already opened.");
        }
    }

    /**
     * Close connection.
     *
     *
     * @throws IOException throws if something error.
     */
    public void close() throws IOException {
        if (state == RMQState.CONNECTED) {
            state = RMQState.CLOSE_PENDING;
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    LOGGER.warn("Failed to close connection.");
                    if (!(e.getCause() instanceof ShutdownSignalException)) {
                        state = RMQState.DISCONNECTED;
                        notifyOnCloseCompleted();
                        connection = null;
                    }
                    throw e;
                }
            }
        } else {
            LOGGER.warn("Connection is already closed.");
        }
    }

    /**
     * Gets if connection is established.
     *
     * @return true if connection is already established.
     */
    public boolean isOpen() {
        return state == RMQState.CONNECTED;
    }

    /**
     * Updates each channels.
     *
     * @param consumeItems
     *            the list of consume items.
     */
    public void updateChannels(Collection<RabbitmqConsumeItem> consumeItems) {
        Collection<String> uniqueQueueNames = new HashSet<String>();

        updatePublishChannel();

        if (consumeItems == null) {
            closeAllConsumeChannels();
        } else {
            // generate unique queue name set
            for (RabbitmqConsumeItem i : consumeItems) {
                uniqueQueueNames.add(i.getQueueName());
            }
            uniqueQueueNames.remove(null);

            // close unused channels
            closeUnusedConsumeChannels(uniqueQueueNames);

            // create channels
            createNewConsumeChannels(uniqueQueueNames, consumeItems);
        }
    }

    /**
     * Creates new channels with specified consume items.
     *
     * @param uniqueQueueNames
     *            the collection of unique queue names.
     * @param consumeItems
     *            the list of consume items.
     */
    private void createNewConsumeChannels(Collection<String> uniqueQueueNames, Collection<RabbitmqConsumeItem> consumeItems) {
        if (state != RMQState.CONNECTED) {
            LOGGER.warn("Cannot create channel because connection is not established.");
            return;
        }

        if (uniqueQueueNames == null || consumeItems == null || uniqueQueueNames.isEmpty() || consumeItems.isEmpty()) {
            LOGGER.info("No create new channel due to empty.");
        } else {
            Collection<String> existingQueueNames = new HashSet<String>();

            // get existing channel name set
            for (ConsumeRMQChannel h : getConsumeRMQChannels()) {
                existingQueueNames.add(h.getQueueName());
            }

            // create non-existing channels
            for (String queueName : uniqueQueueNames) {
                if (!existingQueueNames.contains(queueName)) {
                    Collection<String> appIds = new HashSet<String>();
                    for (RabbitmqConsumeItem i : consumeItems) {
                        if (queueName.equals(i.getQueueName())) {
                            appIds.add(i.getAppId());
                        }
                    }
                    appIds.remove(GlobalRabbitmqConfiguration.CONTENT_NONE);
                    if (!appIds.isEmpty()) {
                        ConsumeRMQChannel ch = new ConsumeRMQChannel(queueName, appIds);
                        ch.addRMQChannelListener(this);
                        try {
                            ch.open(connection);
                            rmqChannels.add(ch);
                        } catch (IOException e) {
                            LOGGER.warn("Failed to open consume channel for {}.", queueName);
                            LOGGER.warn("Exception: {}", e);
                            ch.removeRMQChannelListener(this);
                            continue;
                        }
                    }
                }
            }
        }
    }

    /**
     * Close unused channels.
     *
     * @param usedQueueNames
     *            the collection of used queue names.
     */
    private void closeUnusedConsumeChannels(Collection<String> usedQueueNames) {
        Collection<ConsumeRMQChannel> channels = getConsumeRMQChannels();
        Collection<ConsumeRMQChannel> unclosedChannels = new HashSet<ConsumeRMQChannel>();
        if (!channels.isEmpty()) {
            for (ConsumeRMQChannel ch : channels) {
                if (!usedQueueNames.contains(ch.getQueueName())) {
                    try {
                        ch.close();
                    } catch (IOException | TimeoutException ex) {
                        unclosedChannels.add(ch);
                    }
                }
            }
            if (!unclosedChannels.isEmpty()) {
                for (ConsumeRMQChannel ch : unclosedChannels) {
                    ch.removeRMQChannelListener(this);
                    rmqChannels.remove(ch);
                }
            }
        }
    }

    /**
     * Close all channels.
     */
    private void closeAllChannels() {
        if (!rmqChannels.isEmpty()) {
            Collection<AbstractRMQChannel> unclosedChannels = new HashSet<AbstractRMQChannel>();
            for (AbstractRMQChannel h : rmqChannels) {
                try {
                    h.close();
                } catch (IOException | TimeoutException ex) {
                    unclosedChannels.add(h);
                }
            }
            if (!unclosedChannels.isEmpty()) {
                for (AbstractRMQChannel h : unclosedChannels) {
                    h.removeRMQChannelListener(this);
                    rmqChannels.remove(h);
                }
            }
        }
    }

    /**
     * Close all consume channels.
     */
    private void closeAllConsumeChannels() {
        Collection<ConsumeRMQChannel> channels = getConsumeRMQChannels();
        Collection<ConsumeRMQChannel> unclosedChannels = new HashSet<ConsumeRMQChannel>();
        if (!channels.isEmpty()) {
            for (ConsumeRMQChannel h : channels) {
                try {
                    h.close();
                } catch (IOException | TimeoutException ex) {
                    unclosedChannels.add(h);
                }
            }
            if (!unclosedChannels.isEmpty()) {
                for (ConsumeRMQChannel h : unclosedChannels) {
                    h.removeRMQChannelListener(this);
                    rmqChannels.remove(h);
                }
            }
        }
    }

    /**
     * Update publish channel.
     */
    public void updatePublishChannel() {
        if (getPublishRMQChannels().size() == 0) {
            try {
                PublishRMQChannel pubch = new PublishRMQChannel();
                pubch.addRMQChannelListener(this);
                pubch.open(connection);
                rmqChannels.add(pubch);
            } catch (IOException e) {
                LOGGER.warn("Failed to open publish channel.", e);
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param rmqChannel
     *            the channel.
     */
    public void onOpen(AbstractRMQChannel rmqChannel) {
        if (rmqChannel instanceof ConsumeRMQChannel) {
            ConsumeRMQChannel consumeChannel = (ConsumeRMQChannel) rmqChannel;
            LOGGER.info("Open RabbitMQ channel #{} for {}.",
                    rmqChannel.getChannel().getChannelNumber(),
                    consumeChannel.getQueueName());
            consumeChannel.consume();
        } else if (rmqChannel instanceof PublishRMQChannel) {
            LOGGER.info("Open RabbitMQ channel #{} for publish.",
                    rmqChannel.getChannel().getChannelNumber());
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param rmqChannel
     *            the channel.
     */
    public void onCloseCompleted(AbstractRMQChannel rmqChannel) {
        if (rmqChannels.contains(rmqChannel)) {
            rmqChannel.removeRMQChannelListener(this);
            rmqChannels.remove(rmqChannel);
            try {
                if (rmqChannel instanceof ConsumeRMQChannel) {
                    ConsumeRMQChannel consumeChannel = (ConsumeRMQChannel) rmqChannel;
                    LOGGER.info("Closed RabbitMQ channel #{} for {}.",
                            rmqChannel.getChannel().getChannelNumber(),
                            consumeChannel.getQueueName());
                } else if (rmqChannel instanceof PublishRMQChannel) {
                    LOGGER.info("Closed RabbitMQ channel #{} for publish.",
                            rmqChannel.getChannel().getChannelNumber());
                }
            } catch (Exception ex) {
                // nothing
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param rmqConnectionListener
     *            the connection listener.
     */
    public void addRMQConnectionListener(RMQConnectionListener rmqConnectionListener) {
        rmqConnectionListeners.add(rmqConnectionListener);
    }

    /**
     * {@inheritDoc}
     *
     * @param rmqConnectionListener
     *            the connection listener.
     */
    public void removeRMQConnectionListener(RMQConnectionListener rmqConnectionListener) {
        rmqConnectionListeners.remove(rmqConnectionListener);
    }

    /**
     * {@inheritDoc}
     *
     * @return true if connection is already established.
     */
    public boolean isOpenRMQConnection() {
        return connection.isOpen();
    }

    /**
     * {@inheritDoc}
     *
     * @param event
     *            the event for connection.
     */
    public void notifyRMQConnectionListeners(RMQConnectionEvent event) {
        for (RMQConnectionListener l : rmqConnectionListeners) {
            if (event == RMQConnectionEvent.CLOSE_COMPLETED) {
                l.onCloseCompleted(this);
            } else if (event == RMQConnectionEvent.OPEN) {
                l.onOpen(this);
            }
        }
    }

    /**
     * Notify OnCloseCompleted event.
     */
    public void notifyOnCloseCompleted() {
        notifyRMQConnectionListeners(RMQConnectionEvent.CLOSE_COMPLETED);
    }

    /**
     * Notify OnOpen event.
     */
    public void notifyOnOpen() {
        notifyRMQConnectionListeners(RMQConnectionEvent.OPEN);
    }

    /**
     * {@inheritDoc}
     *
     * @param shutdownSignalException
     *            the exception.
     */
    public void shutdownCompleted(ShutdownSignalException shutdownSignalException) {
        if (shutdownSignalException != null && !shutdownSignalException.isInitiatedByApplication()) {
            LOGGER.warn("RabbitMQ connection was suddenly disconnected.");
            ConnectionMonitor.get().setActivate(true);
        }
        state = RMQState.DISCONNECTED;
        closeAllChannels();
        notifyOnCloseCompleted();
        connection = null;
    }

    //CS IGNORE LineLength FOR NEXT 12 LINES. REASON: Auto generated code.
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((serviceUri == null) ? 0 : serviceUri.hashCode());
        result = prime * result + ((userName == null) ? 0 : userName.hashCode());
        result = prime * result + ((userPassword == null) ? 0 : userPassword.hashCode());
        return result;
    }

    //CS IGNORE LineLength FOR NEXT 38 LINES. REASON: Auto generated code.
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RMQConnection other = (RMQConnection) obj;
        if (serviceUri == null) {
            if (other.serviceUri != null)
                return false;
        } else if (!serviceUri.equals(other.serviceUri))
            return false;
        if (userName == null) {
            if (other.userName != null)
                return false;
        } else if (!userName.equals(other.userName))
            return false;
        if (userPassword == null) {
            if (other.userPassword != null)
                return false;
        } else if (!userPassword.equals(other.userPassword))
            return false;
        return true;
    }
}
