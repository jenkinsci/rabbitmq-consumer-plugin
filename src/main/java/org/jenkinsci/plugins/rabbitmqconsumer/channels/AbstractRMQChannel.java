package org.jenkinsci.plugins.rabbitmqconsumer.channels;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeoutException;

import org.jenkinsci.plugins.rabbitmqconsumer.RMQState;
import org.jenkinsci.plugins.rabbitmqconsumer.events.RMQChannelEvent;
import org.jenkinsci.plugins.rabbitmqconsumer.listeners.RMQChannelListener;
import org.jenkinsci.plugins.rabbitmqconsumer.notifiers.RMQChannelNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * abstract class for handling RabbitMQ channel.
 *
 * @author rinrinne a.k.a. rin_ne
 */
public abstract class AbstractRMQChannel implements RMQChannelNotifier, ShutdownListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRMQChannel.class);

    protected Channel channel;
    protected RMQState state = RMQState.DISCONNECTED;
    protected final Collection<RMQChannelListener> rmqChannelListeners = new CopyOnWriteArraySet<RMQChannelListener>();

    /**
     * Default constructor.
     */
    public AbstractRMQChannel() {
    }

    /**
     * Gets whether channel is opened.
     *
     * @return true if channel is already opened.
     */
    public boolean isOpen() {
        return state == RMQState.CONNECTED;
    }

    /**
     * Open connection.
     *
     * @param connection
     *            the instance of Connection, not RMQConnection.
     * @throws IOException
     *             exception if channel cannot be created.
     */
    public void open(final Connection connection) throws IOException {
        if (state == RMQState.DISCONNECTED) {
            channel = connection.createChannel();
            if (channel != null) {
                state = RMQState.CONNECTED;
                channel.addShutdownListener(this);
                notifyOnOpen();
            }
        } else {
            LOGGER.warn("Channel is already opened or on close pending.");
        }
    }

    /**
     * Gets channel.
     *
     * @return the channel.
     */
    public Channel getChannel() {
        return channel;
    }


    /**
     * Close channel.
     * 
     * @throws IOException
     * @throws TimeoutException
     */
    public void close() throws IOException, TimeoutException {
        if (state == RMQState.CONNECTED) {
            if (channel != null) {
                try {
                    state = RMQState.CLOSE_PENDING;
                    channel.close();
                } catch (IOException | TimeoutException ex) {
                    LOGGER.warn("Failed to close channel.");
                    if (!(ex.getCause() instanceof ShutdownSignalException)) {
                        state = RMQState.DISCONNECTED;
                        notifyOnCloseCompleted();
                        channel = null;
                    }
                    throw ex;
                }
            }
        } else {
            LOGGER.warn("Channel is already closed or on close pending.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param rmqChannelListener
     *            the channel listener.
     */
    public void addRMQChannelListener(RMQChannelListener rmqChannelListener) {
        rmqChannelListeners.add(rmqChannelListener);
    }

    /**
     * {@inheritDoc}
     *
     * @param rmqChannelListener
     *            the channel listener.
     */
    public void removeRMQChannelListener(RMQChannelListener rmqChannelListener) {
        rmqChannelListeners.remove(rmqChannelListener);
    }

    /**
     * {@inheritDoc}
     *
     * @return true if channel is already opened.
     */
    public boolean isOpenRMQChannel() {
        return channel.isOpen();
    }

    /**
     * {@inheritDoc}
     *
     * @param event
     *            the event for channel.
     */
    public void notifyRMQChannelListeners(RMQChannelEvent event) {
        for (RMQChannelListener l : rmqChannelListeners) {
            if (event == RMQChannelEvent.CLOSE_COMPLETED) {
                l.onCloseCompleted(this);
            } else if (event == RMQChannelEvent.OPEN) {
                l.onOpen(this);
            }
        }
    }

    /**
     * Notify OnCloseCompleted event.
     */
    public void notifyOnCloseCompleted() {
        notifyRMQChannelListeners(RMQChannelEvent.CLOSE_COMPLETED);
    }

    /**
     * Notify OnOpen event.
     */
    public void notifyOnOpen() {
        notifyRMQChannelListeners(RMQChannelEvent.OPEN);
    }

    /**
     * {@inheritDoc}
     *
     * @param shutdownSignalException
     *            the exception.
     */
    public void shutdownCompleted(ShutdownSignalException shutdownSignalException) {
        if (shutdownSignalException != null && !shutdownSignalException.isInitiatedByApplication()) {
            LOGGER.warn("RabbitMQ channel {} was suddenly closed.", channel.getChannelNumber());
        }
        state = RMQState.DISCONNECTED;
        notifyOnCloseCompleted();
        channel = null;
    }
}
