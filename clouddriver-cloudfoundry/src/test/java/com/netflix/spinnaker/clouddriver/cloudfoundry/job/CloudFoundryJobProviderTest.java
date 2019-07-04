/*
 * Copyright 2019 Pivotal, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.clouddriver.cloudfoundry.job;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.netflix.spinnaker.clouddriver.cloudfoundry.client.CloudFoundryClient;
import com.netflix.spinnaker.clouddriver.cloudfoundry.client.Logs;
import com.netflix.spinnaker.clouddriver.cloudfoundry.client.Tasks;
import com.netflix.spinnaker.clouddriver.cloudfoundry.client.model.v3.Link;
import com.netflix.spinnaker.clouddriver.cloudfoundry.client.model.v3.Task;
import com.netflix.spinnaker.clouddriver.cloudfoundry.security.CloudFoundryCredentials;
import com.netflix.spinnaker.clouddriver.security.AccountCredentials;
import com.netflix.spinnaker.clouddriver.security.AccountCredentialsProvider;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.cloudfoundry.dropsonde.events.EventFactory.Envelope;
import org.cloudfoundry.dropsonde.events.EventFactory.Envelope.EventType;
import org.cloudfoundry.dropsonde.events.LogFactory.LogMessage;
import org.cloudfoundry.dropsonde.events.LogFactory.LogMessage.MessageType;
import org.junit.jupiter.api.Test;

class CloudFoundryJobProviderTest {
  @Test
  void getFileContents_withNonCloudFoundryAccount_returnsNull() {
    AccountCredentialsProvider credentialProvider = mock(AccountCredentialsProvider.class);
    when(credentialProvider.getCredentials(eq("account1")))
        .thenReturn(mock(AccountCredentials.class));

    CloudFoundryJobProvider provider = new CloudFoundryJobProvider(credentialProvider);
    Map<String, Object> fileContents =
        provider.getFileContents("account1", "location", "jobId", "filename");

    assertThat(fileContents).isNull();
  }

  public AccountCredentialsProvider getFileContentsFixtureSetUp() {
    AccountCredentialsProvider credentialProvider = mock(AccountCredentialsProvider.class);
    CloudFoundryCredentials cfCreds = mock(CloudFoundryCredentials.class);
    CloudFoundryClient cfClient = mock(CloudFoundryClient.class);
    Tasks tasks = mock(Tasks.class);
    Task task =
        Task.builder()
            .guid("taskGuid")
            .name("taskName")
            .links(Collections.singletonMap("app", new Link().setHref("http://apps/appGuid")))
            .build();

    Task otherTask =
        Task.builder()
            .guid("otherTaskGuid")
            .name("otherTaskName")
            .links(Collections.singletonMap("app", new Link().setHref("http://apps/appGuid")))
            .build();

    Logs logsService = mock(Logs.class);

    when(credentialProvider.getCredentials(eq("cfAccount"))).thenReturn(cfCreds);
    when(cfCreds.getClient()).thenReturn(cfClient);
    when(cfClient.getTasks()).thenReturn(tasks);
    when(tasks.getTask(eq("taskGuid"))).thenReturn(task);
    when(tasks.getTask(eq("otherTaskGuid"))).thenReturn(otherTask);
    when(cfClient.getLogs()).thenReturn(logsService);
    when(logsService.recentLogs(eq("appGuid")))
        .thenReturn(
            Arrays.asList(
                Envelope.newBuilder().setOrigin("").setEventType(EventType.ValueMetric).build(),
                Envelope.newBuilder(Envelope.getDefaultInstance())
                    .setEventType(EventType.LogMessage)
                    .setLogMessage(
                        LogMessage.newBuilder()
                            .setMessageType(MessageType.OUT)
                            .setTimestamp(10)
                            .setMessage(ByteString.copyFrom("log message content last", UTF_8))
                            .setSourceType("APP/TASK/taskName")
                            .build())
                    .setOrigin("")
                    .build(),
                Envelope.newBuilder(Envelope.getDefaultInstance())
                    .setEventType(EventType.LogMessage)
                    .setLogMessage(
                        LogMessage.newBuilder()
                            .setMessageType(MessageType.OUT)
                            .setTimestamp(9)
                            .setMessage(ByteString.copyFrom("log message content first", UTF_8))
                            .setSourceType("APP/TASK/taskName")
                            .build())
                    .setOrigin("")
                    .build(),
                Envelope.newBuilder(Envelope.getDefaultInstance())
                    .setEventType(EventType.LogMessage)
                    .setLogMessage(
                        LogMessage.newBuilder()
                            .setMessageType(MessageType.OUT)
                            .setTimestamp(0)
                            .setMessage(ByteString.copyFrom("some message", UTF_8))
                            .setSourceType("APP/TASK/otherTaskName")
                            .build())
                    .setOrigin("")
                    .build()));

    return credentialProvider;
  }

  @Test
  void getFileContents_filterInLogMessagesOnly() {
    CloudFoundryJobProvider provider = new CloudFoundryJobProvider(getFileContentsFixtureSetUp());
    Map<String, Object> fileContents =
        provider.getFileContents("cfAccount", "location", "taskGuid", null);

    List<Map<String, Object>> logs = (List<Map<String, Object>>) fileContents.get("logs");
    for (Map<String, Object> log : logs) {
      assertThat(log.get("message")).isNotNull();
    }
  }

  @Test
  void getFileContents_filterInLogMessagesForSpecifiedTaskOnly() {
    CloudFoundryJobProvider provider = new CloudFoundryJobProvider(getFileContentsFixtureSetUp());
    Map<String, Object> fileContents =
        provider.getFileContents("cfAccount", "location", "otherTaskGuid", null);

    List<Map<String, Object>> logs = (List<Map<String, Object>>) fileContents.get("logs");
    assertThat(logs.size()).isEqualTo(1);

    Map<String, Object> logMessage = logs.get(0);
    assertThat(logMessage.get("message")).isEqualTo("some message");
    assertThat(logMessage.get("sourceType")).isEqualTo("APP/TASK/otherTaskName");
  }

  @Test
  void getFileContents_returnSortedLogMessageByTimestamp() {
    CloudFoundryJobProvider provider = new CloudFoundryJobProvider(getFileContentsFixtureSetUp());
    Map<String, Object> fileContents =
        provider.getFileContents("cfAccount", "location", "taskGuid", null);

    List<Map<String, Object>> logs = (List<Map<String, Object>>) fileContents.get("logs");
    assertThat(logs.size()).isEqualTo(2);
    assertThat(logs.get(0).get("message")).isEqualTo("log message content first");
    assertThat(logs.get(1).get("message")).isEqualTo("log message content last");
  }
}
