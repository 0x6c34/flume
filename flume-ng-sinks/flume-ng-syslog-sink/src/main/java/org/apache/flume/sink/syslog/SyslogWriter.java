package org.apache.flume.sink.syslog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;

public abstract class SyslogWriter {
  private static final Logger logger = LoggerFactory.getLogger(SyslogWriter.class);

  protected InetAddress remote;
  protected int port;

  protected int defaultFacility;
  protected int defaultSeverity;
  protected String defaultHost;
  protected String defaultTag;

  protected boolean usePri;
  protected boolean useHeader;
  protected boolean useTag;

  /** default behavior is truncate */
  protected boolean split = false;

  public abstract void write(final byte[] packet)
  throws IOException;

  public abstract void relay(final boolean force,
                             final int facility,
                             final int severity,
                             final Date date,
                             final String hostname,
                             final byte[] msg)
  throws IOException;

  public abstract void close();

  public InetAddress getRemote() {
    return remote;
  }

  public int getPort() {
    return port;
  }

  public void setSplit(boolean split) {
    this.split = split;
  }

  public boolean isSplit() {
    return split;
  }

  public void usePri(int facility, int severity) {
    this.usePri = true;
    this.defaultFacility = facility;
    this.defaultSeverity = severity;
  }

  public void useHeader(String host) {
    this.useHeader = true;
    this.defaultHost = host;
  }

  public void useTag(String tag) {
    this.useTag = true;
    this.defaultTag = tag;
  }
}
