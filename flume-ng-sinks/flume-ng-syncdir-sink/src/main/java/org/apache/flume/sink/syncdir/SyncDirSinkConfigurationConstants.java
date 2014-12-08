package org.apache.flume.sink.syncdir;

public class SyncDirSinkConfigurationConstants {
  /** Directory to sync. */
  public static final String SYNC_DIRECTORY = "directory";

  /** serializer selection and config */
  public static final String SERIALIZER = "serializer";
  public static final String DEFAULT_SERIALIZER = "TEXT";
  public static final String SERIALIZER_PREFIX = SERIALIZER + ".";

  /** Header in which to put relative filename. */
  public static final String FILENAME_HEADER_KEY = "header";
  public static final String DEFAULT_FILENAME_HEADER_KEY = "path";

  /** What size to batch with before sending to ChannelProcessor. */
  public static final String BATCH_SIZE = "batchSize";
  public static final int DEFAULT_BATCH_SIZE = 100;
}
