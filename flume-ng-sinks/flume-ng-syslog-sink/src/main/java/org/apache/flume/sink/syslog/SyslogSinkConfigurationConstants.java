package org.apache.flume.sink.syslog;

/**
 * This class heavily references IETF rfc3164.
 */
public class SyslogSinkConfigurationConstants {
  /** remote server hostname, must not be null, no defaults */
  public static final String REMOTE = "remote";

  public static final String USE_PRI = "pri";
  public static final boolean DEFAULT_USE_PRI = false;

  public static final String FACILITY_KEY = "key.facility";
  public static final String DEFAULT_FACILITY_KEY = "Facility";

  public static final String FACILITY = "defaults.facility";
  /** user-level messages */
  public static final int DEFAULT_FACILITY = 1;

  public static final String SEVERITY_KEY = "key.severity";
  public static final String DEFAULT_SEVERITY_KEY = "Severity";

  public static final String SEVERITY = "defaults.severity";
  /** Notice: normal but significant condition */
  public static final int DEFAULT_SEVERITY = 5;

  public static final String USE_HEADER = "header";
  public static final boolean DEFAULT_USE_HEADER = false;

  public static final String HOST_KEY = "key.host";
  public static final String DEFAULT_HOST_KEY = "host";

  public static final String HOST = "host";

  public static final String TIMESTAMP_KEY = "key.timestamp";
  public static final String DEFAULT_TIMESTAMP_KEY = "timestamp";

  public static final String USE_TAG = "msg";
  public static final boolean DEFAULT_USE_TAG = false;

  public static final String TAG_KEY = "key.tag";
  public static final String DEFAULT_TAG_KEY = "tag";

  public static final String TAG = "tag";

  /** retry interval in seconds when network is unreachable, defaults to 30 */
  public static final String RETRY_INTERVAL = "retryInterval";
  public static final int DEFAULT_RETRY_INTERVAL = 30;

  /**
   * If set to true, then one message that exceeded 1024 bytes would be split
   * and transferred in multiple network packets. Defaults to "false".
   */
  public static final String SPLIT = "split";
  public static final boolean DEFAULT_SPLIT = false;

  /**
   * how many message's would be processed in one transaction unit, defaults to
   * 100
   */
  public static final String BATCH_SIZE = "batchSize";
  public static final int DEFAULT_BATCH_SIZE = 100;
}
