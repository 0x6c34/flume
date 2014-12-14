package org.apache.flume.sink.syslog;

import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/*
 * Configurable items:
 *  - host: syslog host string, not optional
 *  - sink.batchSize: event processing batch size, optional, defaults to 100
 */
public class SyslogSink extends AbstractSink implements Configurable {
  private static Logger logger;

  //private boolean usePri;
  //private int defaultFacility; // range: 0-23
  //private String facilityKey;
  //private int defaultSeverity; // range: 0-7
  //private String severityKey;
  //private boolean useHeader;
  //private String host;
  //private String hostKey;
  //private String timestampKey;
  //private boolean useTag;
  //private String tag;
  //private String tagKey;
  //private int mode;

  private String remote;
  private boolean split;
  private int retryInterval;

  private int batchSize;

  private SinkCounter sinkCounter = new SinkCounter(getName());

  private SyslogWriter syslogWriter;

  @Override
  public void configure(Context context) {
    /* init logger */
    logger = LoggerFactory.getLogger(SyslogSink.class.getName() + "-" +
        getName());

    boolean usePri = context.getBoolean(
        SyslogSinkConfigurationConstants.USE_PRI,
        SyslogSinkConfigurationConstants.DEFAULT_USE_PRI);
    int defaultFacility = context.getInteger(
        SyslogSinkConfigurationConstants.FACILITY,
        SyslogSinkConfigurationConstants.DEFAULT_FACILITY);
    facilityKey = context.getString(
        SyslogSinkConfigurationConstants.FACILITY_KEY,
        SyslogSinkConfigurationConstants.DEFAULT_FACILITY_KEY);
    defaultSeverity = context.getInteger(
        SyslogSinkConfigurationConstants.SEVERITY,
        SyslogSinkConfigurationConstants.DEFAULT_SEVERITY);
    severityKey = context.getString(
        SyslogSinkConfigurationConstants.SEVERITY_KEY,
        SyslogSinkConfigurationConstants.DEFAULT_SEVERITY_KEY);
    useHeader = context.getBoolean(
        SyslogSinkConfigurationConstants.USE_HEADER,
        SyslogSinkConfigurationConstants.DEFAULT_USE_HEADER);
    host = context.getString(SyslogSinkConfigurationConstants.HOST);
    hostKey = context.getString(
        SyslogSinkConfigurationConstants.HOST_KEY,
        SyslogSinkConfigurationConstants.DEFAULT_HOST_KEY);
    timestampKey = context.getString(
        SyslogSinkConfigurationConstants.TIMESTAMP_KEY,
        SyslogSinkConfigurationConstants.DEFAULT_TIMESTAMP_KEY);
    useTag = context.getBoolean(
        SyslogSinkConfigurationConstants.USE_TAG,
        SyslogSinkConfigurationConstants.DEFAULT_USE_TAG);
    tag = context.getString(
        SyslogSinkConfigurationConstants.TAG);
    tagKey = context.getString(
        SyslogSinkConfigurationConstants.TAG_KEY,
        SyslogSinkConfigurationConstants.DEFAULT_TAG_KEY);

    remote = context.getString(SyslogSinkConfigurationConstants.REMOTE);
    split = context.getBoolean(
        SyslogSinkConfigurationConstants.SPLIT,
        SyslogSinkConfigurationConstants.DEFAULT_SPLIT);
    retryInterval = context.getInteger(
        SyslogSinkConfigurationConstants.RETRY_INTERVAL,
        SyslogSinkConfigurationConstants.DEFAULT_RETRY_INTERVAL);

    batchSize = context.getInteger(
        SyslogSinkConfigurationConstants.BATCH_SIZE,
        SyslogSinkConfigurationConstants.DEFAULT_BATCH_SIZE);

    /* check config */
    Preconditions.checkArgument(defaultFacility >= 0 && defaultFacility <= 23,
        "facility out of range");
    Preconditions.checkArgument(defaultSeverity >= 0 && defaultSeverity <= 7,
        "severity out of range");
    Preconditions.checkNotNull(remote, "syslog remote host must not be null");
    /* assign default hostname */
    if (null == host) {
      try {
        host = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        host = "localhost";
      }
    }
    /* assign meta part */
    mode = usePri ? (mode | SyslogWriter.PRI) : mode;
    mode = useHeader ? (mode | SyslogWriter.HEADER) : mode;
    mode = useTag ? (mode | SyslogWriter.TAG) : mode;

    sinkCounter = new SinkCounter(this.getName());
  }

  @Override
  public synchronized void start() {
    logger.info("syslog sink {} starting...", getName());

    // network transport start
    try {
      syslogWriter = SyslogWriterFactory.getInstance(host);
    } catch (SyslogException e) {
      sinkCounter.incrementConnectionFailedCount();
      throw new FlumeException("error while retrieving Syslog writer with " +
          "host string: " + host, e);
    }
    syslogWriter.setSplit(split);

    // flume sink start
    super.start();
    sinkCounter.incrementConnectionCreatedCount();
    sinkCounter.start();

    logger.info("syslog sink {} started", getName());
  }

  @Override
  public synchronized void stop() {
    logger.info("syslog sink {} stopping...", getName());

    // close SyslogWriter
    if (syslogWriter != null)
      syslogWriter.close();
    sinkCounter.incrementConnectionClosedCount();

    // flume sink stop
    sinkCounter.stop();
    super.stop();

    logger.info("syslog sink {} stopped", getName());
  }

  @Override
  public Status process()
  throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();
    Event event = null;
    Status result = Status.READY;
    txn.begin();

    try {
      long i = 0;
      for (; i < batchSize; i++) {
        event = channel.take();
        if (event == null) {
          // No events found, request back-off semantics from runner
          result = Status.BACKOFF;
          if (i == 0) {
            sinkCounter.incrementBatchEmptyCount();
          } else {
            sinkCounter.incrementBatchUnderflowCount();
          }
          break;
        } else {
          sinkCounter.incrementEventDrainAttemptCount();
          Map<String, String> headers = event.getHeaders();
          if (usePri) {
            if (!headers.containsKey(facilityKey)) {
              // TODO:
            }
          }
        }
      }
      if (i == batchSize) {
        sinkCounter.incrementBatchCompleteCount();
      }
      sinkCounter.addToEventDrainAttemptCount(i);

      txn.commit();
      sinkCounter.addToEventDrainSuccessCount(i);
    } catch (IOException e) {
      txn.rollback();
      throw new EventDeliveryException("Failed to process transaction due to " +
          "IO error", e);
    } finally {
      txn.close();
    }

    return result;
  }
}
