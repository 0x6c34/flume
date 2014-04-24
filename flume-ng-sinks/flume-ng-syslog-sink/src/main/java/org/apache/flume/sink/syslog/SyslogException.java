package org.apache.flume.sink.syslog;

public class SyslogException extends Exception {
  public SyslogException(String message) {
    super(message);
  }

  public SyslogException(Throwable cause) {
    super(cause);
  }

  public SyslogException(String message, Throwable cause) {
    super(message, cause);
  }
}
