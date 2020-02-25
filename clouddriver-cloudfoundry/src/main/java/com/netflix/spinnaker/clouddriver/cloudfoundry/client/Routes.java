/*
 * Copyright 2018 Pivotal, Inc.
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

package com.netflix.spinnaker.clouddriver.cloudfoundry.client;

import static com.netflix.spinnaker.clouddriver.cloudfoundry.client.CloudFoundryClientUtils.collectPageResources;
import static com.netflix.spinnaker.clouddriver.cloudfoundry.client.CloudFoundryClientUtils.safelyCall;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.groupingBy;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.spinnaker.clouddriver.cloudfoundry.client.api.RouteService;
import com.netflix.spinnaker.clouddriver.cloudfoundry.client.model.RouteId;
import com.netflix.spinnaker.clouddriver.cloudfoundry.client.model.v2.Resource;
import com.netflix.spinnaker.clouddriver.cloudfoundry.client.model.v2.Route;
import com.netflix.spinnaker.clouddriver.cloudfoundry.client.model.v2.RouteMapping;
import com.netflix.spinnaker.clouddriver.cloudfoundry.model.CloudFoundryDomain;
import com.netflix.spinnaker.clouddriver.cloudfoundry.model.CloudFoundryLoadBalancer;
import com.netflix.spinnaker.clouddriver.cloudfoundry.model.CloudFoundryServerGroup;
import com.netflix.spinnaker.clouddriver.cloudfoundry.model.CloudFoundrySpace;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Routes {
  private static final Pattern VALID_ROUTE_REGEX =
      Pattern.compile("^([a-zA-Z0-9_-]+)\\.([a-zA-Z0-9_.-]+)(:[0-9]+)?([/a-zA-Z0-9_-]+)?$");

  private final String account;
  private final RouteService api;
  private final Applications applications;
  private final Domains domains;
  private final Spaces spaces;
  private final Integer resultsPerPage;

  private final ForkJoinPool forkJoinPool;
  private LoadingCache<String, List<RouteMapping>> routeMappings;

  public Routes(
      String account,
      RouteService api,
      Applications applications,
      Domains domains,
      Spaces spaces,
      Integer resultsPerPage,
      int maxConnections) {
    this.account = account;
    this.api = api;
    this.applications = applications;
    this.domains = domains;
    this.spaces = spaces;
    this.resultsPerPage = resultsPerPage;

    this.forkJoinPool = new ForkJoinPool(maxConnections);
    this.routeMappings =
        CacheBuilder.newBuilder()
            .expireAfterWrite(120, TimeUnit.SECONDS)
            .build(
                new CacheLoader<String, List<RouteMapping>>() {
                  @Override
                  public List<RouteMapping> load(@Nonnull String guid)
                      throws CloudFoundryApiException, ResourceNotFoundException {
                    log.info("individual route mapping loading {}", guid);
                    return collectPageResources("route mappings", pg -> api.routeMappings(guid, pg))
                        .stream()
                        .map(Resource::getEntity)
                        .collect(Collectors.toList());
                  }

                  @Override
                  public Map<String, List<RouteMapping>> loadAll(Iterable<? extends String> keys) {
                    return collectPageResources("all route mappings", api::allRouteMappings)
                        .parallelStream()
                        .map(Resource::getEntity)
                        .collect(groupingBy(RouteMapping::getRouteGuid));
                  }
                });
  }

  private CloudFoundryLoadBalancer map(Resource<Route> routeResource)
      throws CloudFoundryApiException {
    final String routeGuid = routeResource.getMetadata().getGuid();
    final Route route = routeResource.getEntity();

    Set<CloudFoundryServerGroup> mappedApps = emptySet();
    try {
      mappedApps =
          routeMappings.get(routeGuid).stream()
              .map(rm -> applications.findById(rm.getAppGuid()))
              .collect(Collectors.toSet());
    } catch (ExecutionException e) {
      if (!(e.getCause() instanceof ResourceNotFoundException))
        throw new CloudFoundryApiException(e.getCause(), "Unable to find route mappings by id");
    }

    return CloudFoundryLoadBalancer.builder()
        .account(account)
        .id(routeGuid)
        .host(route.getHost())
        .path(route.getPath())
        .port(route.getPort())
        .space(spaces.findById(route.getSpaceGuid()))
        .domain(domains.findById(route.getDomainGuid()))
        .mappedApps(mappedApps)
        .build();
  }

  @Nullable
  public CloudFoundryLoadBalancer find(RouteId routeId, String spaceId)
      throws CloudFoundryApiException {
    CloudFoundrySpace id = spaces.findById(spaceId);
    String orgId = id.getOrganization().getId();

    List<String> queryParams = new ArrayList<>();
    queryParams.add("host:" + routeId.getHost());
    queryParams.add("organization_guid:" + orgId);
    queryParams.add("domain_guid:" + routeId.getDomainGuid());
    if (routeId.getPath() != null) queryParams.add("path:" + routeId.getPath());
    if (routeId.getPort() != null) queryParams.add("port:" + routeId.getPort().toString());

    return collectPageResources("route mappings", pg -> api.all(pg, 1, queryParams)).stream()
        .filter(
            routeResource ->
                (routeId.getPath() != null || routeResource.getEntity().getPath().isEmpty())
                    && (routeId.getPort() != null || routeResource.getEntity().getPort() == null))
        .findFirst()
        .map(this::map)
        .orElse(null);
  }

  @Nullable
  public RouteId toRouteId(String uri) throws CloudFoundryApiException {
    Matcher matcher = VALID_ROUTE_REGEX.matcher(uri);
    if (matcher.find()) {
      CloudFoundryDomain domain = domains.findByName(matcher.group(2)).orElse(null);
      if (domain == null) {
        return null;
      }
      RouteId routeId = new RouteId();
      routeId.setHost(matcher.group(1));
      routeId.setDomainGuid(domain.getId());
      routeId.setPort(
          matcher.group(3) == null ? null : Integer.parseInt(matcher.group(3).substring(1)));
      routeId.setPath(matcher.group(4));
      return routeId;
    } else {
      return null;
    }
  }

  public List<CloudFoundryLoadBalancer> all() throws CloudFoundryApiException {
    try {
      log.info("LB all start");
      return forkJoinPool
          .submit(
              () -> {
                try {
                  // Force the cache to load all the route mappings in a few paged calls
                  // instead of doing 1 HTTP call per route/mapping
                  log.info("lb mapping all pages");
                  routeMappings.getAll(Collections.singleton(""));
                } catch (CacheLoader.InvalidCacheLoadException ignored) {
                }
                log.info("lb mapping all pages DONE");
                log.info("collecting all routes pages");
                List<Resource<Route>> routes =
                    collectPageResources("routes", pg -> api.all(pg, resultsPerPage, null));
                log.info("collecting all routes pages DONE");
                List<CloudFoundryLoadBalancer> collected =
                    routes.parallelStream().map(this::map).collect(Collectors.toList());
                log.info("route collected");
                return collected;
              })
          .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public CloudFoundryLoadBalancer createRoute(RouteId routeId, String spaceId)
      throws CloudFoundryApiException {
    Route route = new Route(routeId, spaceId);
    try {
      Resource<Route> newRoute =
          safelyCall(() -> api.createRoute(route))
              .orElseThrow(
                  () ->
                      new CloudFoundryApiException(
                          "Cloud Foundry signaled that route creation succeeded but failed to provide a response."));
      return map(newRoute);
    } catch (CloudFoundryApiException e) {
      if (e.getErrorCode() == null) throw e;

      switch (e.getErrorCode()) {
        case ROUTE_HOST_TAKEN:
        case ROUTE_PATH_TAKEN:
        case ROUTE_PORT_TAKEN:
          return this.find(routeId, spaceId);
        default:
          throw e;
      }
    }
  }

  public void deleteRoute(String loadBalancerGuid) throws CloudFoundryApiException {
    safelyCall(() -> api.deleteRoute(loadBalancerGuid));
  }

  public static boolean isValidRouteFormat(String route) {
    return VALID_ROUTE_REGEX.matcher(route).find();
  }
}
