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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.spinnaker.clouddriver.cloudfoundry.CloudFoundryCloudProvider;
import com.netflix.spinnaker.clouddriver.cloudfoundry.client.CloudFoundryClient;
import com.netflix.spinnaker.clouddriver.cloudfoundry.client.model.v3.Task;
import com.netflix.spinnaker.clouddriver.cloudfoundry.security.CloudFoundryCredentials;
import com.netflix.spinnaker.clouddriver.model.JobProvider;
import com.netflix.spinnaker.clouddriver.security.AccountCredentials;
import com.netflix.spinnaker.clouddriver.security.AccountCredentialsProvider;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.cloudfoundry.dropsonde.events.EventFactory.Envelope;
import org.cloudfoundry.dropsonde.events.EventFactory.Envelope.EventType;
import org.cloudfoundry.dropsonde.events.LogFactory.LogMessage;
import org.springframework.stereotype.Component;

@Component
public class CloudFoundryJobProvider implements JobProvider<CloudFoundryJobStatus> {

  @Getter private String platform = CloudFoundryCloudProvider.ID;
  private final AccountCredentialsProvider accountCredentialsProvider;

  public CloudFoundryJobProvider(AccountCredentialsProvider accountCredentialsProvider) {
    this.accountCredentialsProvider = accountCredentialsProvider;
  }

  @Override
  public CloudFoundryJobStatus collectJob(String account, String location, String id) {
    CloudFoundryClient client = getClient(account);
    if (client == null) {
      return null;
    }

    Task task = client.getTasks().getTask(id);
    return CloudFoundryJobStatus.fromTask(task, account, location);
  }

  @Override
  public Map<String, Object> getFileContents(
      String account, String location, String id, String fileName) {
    CloudFoundryClient client = getClient(account);
    if (client == null) {
      return null;
    }

    Task task = client.getTasks().getTask(id);
    String appGuid = task.getLinks().get("app").getGuid();
    String taskLogSourceTypeFilter = String.format("APP/TASK/%s", task.getName());

    Printer printer = JsonFormat.printer();
    ObjectMapper objectMapper = new ObjectMapper();

    List<Map<String, Object>> logsMessages =
        client.getLogs().recentLogs(appGuid).stream()
            .filter(e -> e.getEventType().equals(EventType.LogMessage))
            .map(Envelope::getLogMessage)
            .filter(logMessage -> taskLogSourceTypeFilter.equals(logMessage.getSourceType()))
            .sorted(Comparator.comparingLong(LogMessage::getTimestamp))
            // Converting to JSON first using protobuf's own JsonFormat library as it's otherwise
            // pretty tricky to convert straight com.google.protobuf.MessageOrBuilder to
            // java.util.Map using Jackson's ObjectMapper.convertValue()
            .map(
                logMessage -> {
                  try {
                    return printer.print(logMessage);
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                })
            .map(
                jsonLogMessage -> {
                  try {
                    return (Map<String, Object>) objectMapper.readValue(jsonLogMessage, Map.class);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                })
            .peek(
                logMessage -> {
                  String decodedMessage =
                      new String(
                          BaseEncoding.base64().decode((String) logMessage.get("message")),
                          StandardCharsets.UTF_8);

                  logMessage.put("message", decodedMessage);
                })
            .collect(Collectors.toList());

    return Collections.singletonMap("logs", logsMessages);
  }

  @Override
  public void cancelJob(String account, String location, String taskGuid) {
    CloudFoundryClient client = getClient(account);
    if (client == null) {
      return;
    }

    client.getTasks().cancelTask(taskGuid);
  }

  private CloudFoundryClient getClient(String accountName) {
    AccountCredentials credentials = accountCredentialsProvider.getCredentials(accountName);
    if (!(credentials instanceof CloudFoundryCredentials)) {
      return null;
    }
    return ((CloudFoundryCredentials) credentials).getClient();
  }
}
