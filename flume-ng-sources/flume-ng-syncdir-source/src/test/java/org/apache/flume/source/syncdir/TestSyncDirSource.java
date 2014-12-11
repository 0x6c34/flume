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

import com.google.common.io.Files;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestSyncDirSource {
  private File tmpDir;

  @Before
  public void setUp() throws IOException {
    tmpDir = Files.createTempDir();
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(tmpDir);
  }

  @Test
  public void testLifeCycle() throws IOException, InterruptedException {
    SyncDirSource source = new SyncDirSource();
    MemoryChannel channel = new MemoryChannel();

    // configure source
    Context sourceContext = new Context();
    sourceContext.put(SyncDirSourceConfigurationConstants.SYNC_DIRECTORY,
        tmpDir.toString());
    Configurables.configure(source, sourceContext);

    // configure channel
    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);
    ChannelSelector channelSelector = new ReplicatingChannelSelector();
    channelSelector.setChannels(channels);
    source.setChannelProcessor(new ChannelProcessor(channelSelector));

    // init file
    File f1 = File.createTempFile("syncdir_testLifeCycle", null, tmpDir);
    String line = "file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
        "file1line5\nfile1line6\nfile1line7\nfile1line8\n"; // 8 lines
    Files.write(line.getBytes(), f1);

    // read
    for (int i = 0; i < 10; i++) {
      source.start();

      Assert.assertTrue("Reached start or error", LifecycleController.waitForOneOf(
          source, LifecycleState.START_OR_ERROR));
      Assert.assertEquals("Server is started", LifecycleState.START,
          source.getLifecycleState());

      source.stop();
      Assert.assertTrue("Reached stop or error",
          LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
      Assert.assertEquals("Server is stopped", LifecycleState.STOP,
          source.getLifecycleState());
    }
  }

  @Test
  public void testRead() throws IOException, InterruptedException {
    SyncDirSource source = new SyncDirSource();
    MemoryChannel channel = new MemoryChannel();

    // configure source
    Context sourceContext = new Context();
    sourceContext.put(SyncDirSourceConfigurationConstants.SYNC_DIRECTORY,
        tmpDir.toString());
    sourceContext.put(SyncDirSourceConfigurationConstants.BATCH_SIZE, "2");
    Configurables.configure(source, sourceContext);

    // configure channel
    Context channelContext = new Context();
    channelContext.put("capacity", "2");
    channelContext.put("transactionCapacity", "2");
    Configurables.configure(channel, channelContext);
    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);
    ChannelSelector channelSelector = new ReplicatingChannelSelector();
    channelSelector.setChannels(channels);
    source.setChannelProcessor(new ChannelProcessor(channelSelector));

    // init file
    File file = File.createTempFile("syncdir_testNormalRead", null, tmpDir);
    String line = "file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
        "file1line5\nfile1line6\nfile1line7\nfile1line8\n"; // 8 lines
    Files.write(line.getBytes(), file);

    // test read
    source.start();
    // Wait for the source to read enough events to fill up the channel.
    while (!source.hitChannelException())
      Thread.sleep(50);

    int i, j = 0;
    Event e = null;
    for (i = 0; i < 5; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      if ((e = channel.take()) != null) j++;
      if ((e = channel.take()) != null) j++;
      txn.commit();
      txn.close();
    }
    Assert.assertTrue("Expected to hit ChannelException, but did not!",
        source.hitChannelException());
    System.out.println("normal read lines: " + j);
    Assert.assertEquals(8, j);
    channel.stop();
    source.stop();

  }
}
