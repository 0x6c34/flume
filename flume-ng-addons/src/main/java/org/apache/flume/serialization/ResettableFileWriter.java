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

package org.apache.flume.serialization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * A file writer read lines out of file each time and has the capabilities to
 * reset its current writing position to a previously marked one.
 */
public class ResettableFileWriter {
  private static final Logger logger = LoggerFactory.getLogger(ResettableFileWriter
      .class);
  private File file;
  private FileChannel ch;
  private ByteBuffer byteBuffer;
  private ByteArrayOutputStream outputStream;
  private File statsFile;
  private FileOutputStream statsFileOut;
  private long markedPosition = 0;
  private long writingPosition = 0;
  private boolean statsFileCorrupted = false;

  /**
   * @param file            to read should be treated as ended, no more reading
   *                        after this batch. if false, always reading from this
   *                        file
   * @param statsFileSuffix
   * @throws IOException
   */
  public ResettableFileWriter(File file,
                              String statsFilePrefix,
                              String statsFileSuffix) throws IOException {
    this.file = file;
    if (file.exists() && file.isDirectory())
      throw new IOException("file '" + file + "' is a directory");
    ch = new FileOutputStream(file).getChannel();

    /* stats file */
    statsFile = new File(file.getParent(), statsFilePrefix + file.getName() + statsFileSuffix);

    /* get previous line position */
    retrieveStats();
  }

  /** Retrieve previous line position. */
  private void retrieveStats() throws IOException {
    logger.debug("retrieving status for file '{}'", file);
    if (statsFile.exists()) {
      logger.debug("found stats file: '{}'", statsFile);
      BufferedReader reader = new BufferedReader(new FileReader(statsFile));
      List<String> lines = new ArrayList<String>();
      try {
        String line = null;
        while ((line = reader.readLine()) != null) {
          lines.add(line);
        }
      } finally {
        reader.close();
      }
      if (lines.size() == 0 || lines.get(0).length() == 0) {
        logger.error("stats file '{}' corrupted, reset position to 0",
            statsFile);
        purgeStatFile();
      }
      try {
        writingPosition = markedPosition = Long.valueOf(lines.get(0));
        ch.position(markedPosition);
      } catch (NumberFormatException e) {
        logger.warn("stats file '{}' format error, reset stat file",
            file.getAbsolutePath());
        purgeStatFile();
      }
    }
    logger.debug("got line number '{}'", statsFile, markedPosition);
  }

  private void ensureOpen() throws IOException {
    if (!ch.isOpen()) {
      throw new IOException("the channel of file '" + file + "' is closed");
    }
  }

  public int write(final byte[] chars) throws IOException {
    /* this file was already marked as finished, EOF now */
    if (statsFileCorrupted) return -1;
    ensureOpen();

    long initWritingPosition = writingPosition;

    if (null == byteBuffer) {
      byteBuffer = ByteBuffer.allocate(128 * 1024); // 128KB
    }

    byteBuffer.clear();
    for (byte b : chars) {
      if (!byteBuffer.hasRemaining()) {
        byteBuffer.flip();
        writingPosition += ch.write(byteBuffer);
        byteBuffer.clear();
      }
      byteBuffer.put(b);
    }
    byteBuffer.flip();
    writingPosition += ch.write(byteBuffer);
    return (int) (writingPosition - initWritingPosition);
  }

  public void close() throws IOException {
    if (null != ch)
      ch.close();
    if (null != statsFileOut)
      statsFileOut.close();
    logger.debug("close stat file: '{}'", file);
  }

  private void purgeStatFile() throws IOException {
    statsFileOut.close();
    statsFileOut = null;
    statsFile.delete();
  }

  /** Record the position of current reading file into stats file. */
  public void commit() throws IOException {
    if (statsFileCorrupted) {
      logger.warn("stats file '{}' corrupted, do nothing", statsFile);
      return;
    }

    /* open stat file for write */
    try {
      if (null == statsFileOut) {
        statsFileOut = new FileOutputStream(statsFile, false);
        statsFileCorrupted = false;
      }
    } catch (IOException ioe) {
      statsFileCorrupted = true;
      throw new IOException("cannot create stats file for log file '" + file +
          "', this class needs stats file to function normally", ioe);
    }
    statsFileOut.getChannel().position(0);
    statsFileOut.write(String.valueOf(writingPosition).getBytes());
    statsFileOut.flush();
    markedPosition = writingPosition;
    logger.trace("committed '{}', position: {}", statsFile, markedPosition);
  }

  /** Rewind reading position to previous recorded position. */
  public void reset() throws IOException {
    logger.debug(": {}", file);
    if (statsFileCorrupted) {
      logger.warn("stats file '{}' corrupted, do nothing", statsFile);
      return;
    }

    writingPosition = markedPosition;
    ch.position(markedPosition);
    logger.info("file '{}': reverted to previous marked position: [{}]",
        file, String.valueOf(markedPosition));
  }

  public File getFile() {
    return file;
  }

}
