package org.jenkinsci.plugins.rabbitmqconsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jenkinsci.plugins.rabbitmqconsumer.events.RMQChannelEvent;
import org.jenkinsci.plugins.rabbitmqconsumer.listeners.RMQChannelListener;
import org.jenkinsci.plugins.rabbitmqconsumer.notifiers.RMQChannelNotifier;
import org.jenkinsci.plugins.rabbitmqconsumer.utils.ApplicationMessageNotifyUtil;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * Handle class for RabbitMQ channel.
 * 
 * @author rinrinne a.k.a. rin_ne
 */
public class RMQChannel implements RMQChannelNotifier, ShutdownListener {

    private static final String CONTENT_TYPE_JSON = "application/json";

    private static final Logger LOGGER = Logger.getLogger(RMQChannel.class.getName());

    private Channel channel;
    private final HashSet<String> appIds;
    private final String queueName;
    private final List<RMQChannelListener> rmqChannelListeners = new ArrayList<RMQChannelListener>();
    private volatile boolean consumeStarted = false;

    private final boolean debug = GlobalRabbitmqConfiguration.get().isEnableDebug();
    @SuppressWarnings("serial")
    private final HashSet<String> debugId = new HashSet<String>() {
        {
            add(RabbitmqConsumeItem.DEBUG_APPID);
        }
    };

    private Object lock = new Object();

    /**
     * Creates instance with specified parameters.
     * 
     * @param queueName
     *            the queue name.
     * @param appIds
     *            the hashset of application id.
     */
    public RMQChannel(String queueName, HashSet<String> appIds) {
        this.queueName = queueName;
        this.appIds = appIds;
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
        channel = connection.createChannel();
        channel.addShutdownListener(this);
        notifyOnOpen();
    }

    /**
     * Gets instance of Channel, not RMQChannel.
     * 
     * @return the instance of Channel.
     */
    public Channel getChannel() {
        return channel;
    }

    /**
     * Get hashset of app ids.
     * 
     * @return the hashset of app ids.
     */
    public HashSet<String> getAppIds() {
        return appIds;
    }

    /**
     * Gets queue name.
     * 
     * @return the queue name.
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * Close channel.
     */
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            LOGGER.warning("Could not close channel. but go forward.");
        }
    }

    /**
     * Starts consume.
     */
    public void consume() {
        try {
            channel.basicConsume(queueName, false, new MessageConsumer(channel));
            consumeStarted = true;
            ApplicationMessageNotifyUtil.fireOnBind(appIds, queueName);
        } catch (IOException e) {
            LOGGER.info(e.toString());
        }
    }

    /**
     * Gets whether consumer is already started or not.
     * 
     * @return true if consumer is already started.
     */
    public boolean isConsumeStarted() {
        return consumeStarted;
    }

    /**
     * Handle class that consume message.
     * 
     * @author rinrinne a.k.a. rin_ne
     * 
     */
    public class MessageConsumer extends DefaultConsumer {

        /**
         * Creates instance with specified parameter.
         * 
         * @param channel
         *            the instance of Channel, not RMQChannel.
         */
        public MessageConsumer(Channel channel) {
            super(channel);
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
                throws IOException {

            try {

                long deliveryTag = envelope.getDeliveryTag();

                if (!properties.getAppId().equals(RabbitmqConsumeItem.DEBUG_APPID)) {
                    if (debug) {
                        ApplicationMessageNotifyUtil.fireOnReceive(debugId, queueName, new String(body, "UTF-8"));
                    }
                    if (CONTENT_TYPE_JSON.equals(properties.getContentType()) && appIds.contains(properties.getAppId())) {
                        ApplicationMessageNotifyUtil.fireOnReceive(appIds, queueName, new String(body, "UTF-8"));
                    }
                }

                channel.basicAck(deliveryTag, false);

            } catch (IOException e) {
                throw e;
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "caught exception in delivery handler", e);
            }
        }
    }

    /**
     * @inheritDoc
     * @param rmqChannelListener
     *            the channel listener.
     */
    public void addRMQChannelListener(RMQChannelListener rmqChannelListener) {
        synchronized (lock) {
            if (!rmqChannelListeners.contains(rmqChannelListener)) {
                rmqChannelListeners.add(rmqChannelListener);
            }
        }
    }

    /**
     * @inheritDoc
     * @param rmqChannelListener
     *            the channel listener.
     */
    public void removeRMQChannelListener(RMQChannelListener rmqChannelListener) {
        synchronized (lock) {
            rmqChannelListeners.remove(rmqChannelListener);
        }
    }

    /**
     * @inheritDoc
     * @return true if channel is already opened.
     */
    public boolean isOpenRMQChannel() {
        return channel.isOpen();
    }

    /**
     * @inheritDoc
     * @param event
     *            the event for channel.
     */
    public void notifyRMQChannelListeners(RMQChannelEvent event) {
        synchronized (lock) {
            for (RMQChannelListener l : rmqChannelListeners) {
                if (event == RMQChannelEvent.CLOSE_COMPLETED) {
                    l.onCloseCompleted(this);
                } else if (event == RMQChannelEvent.OPEN) {
                    l.onOpen(this);
                }
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
     * @inheritDoc
     * @param shutdownSignalException
     *            the exception.
     */
    public void shutdownCompleted(ShutdownSignalException shutdownSignalException) {
        channel = null;
        consumeStarted = false;
        ApplicationMessageNotifyUtil.fireOnUnbind(appIds, queueName);
        notifyOnCloseCompleted();
    }
}
