/*
   Copyright 2013 Vincent.Gu

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package org.apache.flume.source.syncdir;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * SyncDirSource is a source that will sync files like spool directory
 * but also copy the directory's original layout. Also unlike spool
 * directory, this source will also track changed files.
 * <p/>
 * For e.g., a file will be identified as finished and stops reading from it
 * if an empty file with suffix  ".done" that present in the same directory of
 * the same name as of the original file.
 */
public class SyncDirSource extends AbstractSource implements
    Configurable, EventDrivenSource {

  private static final Logger logger = LoggerFactory
      .getLogger(SyncDirSource.class);
  // Delay used when polling for file changes
  private int poll_delay_ms = 2000;
  /* Config options */
  private File syncDirectory;
  private String directoryPrefix;
  private String endFileSuffix;
  private String statsFilePrefix;
  private String syncingStatsFileSuffix;
  private String syncedStatsFileSuffix;
  private String filenameHeaderKey = SyncDirSourceConfigurationConstants.FILENAME_HEADER_KEY;
  private int batchSize;
  private ScheduledExecutorService executor;
  private CounterGroup counterGroup;
  private Runnable runner;
  private SyncDirFileLineReader reader;

  @Override
  public synchronized void start() {
    logger.info("SyncDirSource source starting with directory:{}",
        syncDirectory);

    executor = Executors.newSingleThreadScheduledExecutor();
    counterGroup = new CounterGroup();

    reader = new SyncDirFileLineReader(
        syncDirectory, endFileSuffix,
        statsFilePrefix, syncingStatsFileSuffix, syncedStatsFileSuffix);
    runner = new DirectorySyncRunnable(reader, counterGroup);

    executor.scheduleWithFixedDelay(
        runner, 0, poll_delay_ms, TimeUnit.MILLISECONDS);

    super.start();
    logger.debug("SyncDirSource source started");
  }

  @Override
  public synchronized void stop() {
    super.stop();
    logger.debug("SyncDirSource source stopped");
  }

  @Override
  public void configure(Context context) {
    String syncDirectoryStr = context.getString(
        SyncDirSourceConfigurationConstants.SYNC_DIRECTORY);
    Preconditions.checkState(syncDirectoryStr != null,
        "Configuration must specify a sync directory");
    syncDirectory = new File(syncDirectoryStr);

    directoryPrefix = context.getString(
        SyncDirSourceConfigurationConstants.DIRECTORY_PREFIX,
        SyncDirSourceConfigurationConstants.DEFAULT_DIRECTORY_PREFIX);
    endFileSuffix = context.getString(
        SyncDirSourceConfigurationConstants.END_FILE_SUFFIX,
        SyncDirSourceConfigurationConstants.DEFAULT_END_FILE_SUFFIX);
    statsFilePrefix = context.getString(
        SyncDirSourceConfigurationConstants.STATS_FILE_PREFIX,
        SyncDirSourceConfigurationConstants.DEFAULT_STATS_FILE_PREFIX);
    syncingStatsFileSuffix = context.getString(
        SyncDirSourceConfigurationConstants.SYNCING_STATS_FILE_SUFFIX,
        SyncDirSourceConfigurationConstants.DEFAULT_SYNCING_STATS_FILE_SUFFIX);
    syncedStatsFileSuffix = context.getString(
        SyncDirSourceConfigurationConstants.SYNCED_STATS_FILE_SUFFIX,
        SyncDirSourceConfigurationConstants.DEFAULT_SYNCED_STATS_FILE_SUFFIX);
    batchSize = context.getInteger(
        SyncDirSourceConfigurationConstants.BATCH_SIZE,
        SyncDirSourceConfigurationConstants.DEFAULT_BATCH_SIZE);
    poll_delay_ms = context.getInteger(
        SyncDirSourceConfigurationConstants.POLL_DELAY_MS,
        SyncDirSourceConfigurationConstants.DEFAULT_POLL_DELAY_MS);
  }

  private Event createEvent(byte[] lineEntry, String filename) {
    Event out = EventBuilder.withBody(lineEntry);
    if (directoryPrefix.length() > 0) {
      out.getHeaders().put(filenameHeaderKey,
          directoryPrefix + File.separator + filename);
    } else {
      out.getHeaders().put(filenameHeaderKey, filename);
    }
    return out;
  }

  private class DirectorySyncRunnable implements Runnable {
    private SyncDirFileLineReader reader;
    private CounterGroup counterGroup;

    public DirectorySyncRunnable(SyncDirFileLineReader reader,
                                 CounterGroup counterGroup) {
      this.reader = reader;
      this.counterGroup = counterGroup;
    }

    @Override
    public void run() {
      try {
        while (true) {
          List<byte[]> lines = reader.readLines(batchSize);
          if (lines.size() == 0) {
            break;
          }
          String file = syncDirectory.toURI().relativize(
              reader.getLastFileRead().toURI()).getPath();
          List<Event> events = Lists.newArrayList();
          for (byte[] l : lines) {
            counterGroup.incrementAndGet("syncdir.lines.read");
            events.add(createEvent(l, file));
          }
          getChannelProcessor().processEventBatch(events);
          reader.commit();
        }
      } catch (Throwable t) {
        logger.error("Uncaught exception in Runnable", t);
        if (t instanceof Error) {
          throw (Error) t;
        }
      }
    }
  }
}
