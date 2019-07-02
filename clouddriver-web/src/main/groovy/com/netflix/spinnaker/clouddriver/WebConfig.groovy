/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.clouddriver

import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.clouddriver.configuration.CredentialsConfiguration
import com.netflix.spinnaker.clouddriver.controllers.AccountPathVariable
import com.netflix.spinnaker.clouddriver.requestqueue.RequestQueue
import com.netflix.spinnaker.clouddriver.requestqueue.RequestQueueConfiguration
import com.netflix.spinnaker.clouddriver.security.AccountCredentials
import com.netflix.spinnaker.clouddriver.security.AccountCredentialsProvider
import com.netflix.spinnaker.filters.AuthenticatedRequestFilter
import com.netflix.spinnaker.kork.dynamicconfig.DynamicConfigService
import com.netflix.spinnaker.kork.web.interceptors.MetricsInterceptor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.web.servlet.FilterRegistrationBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.core.MethodParameter
import org.springframework.core.Ordered
import org.springframework.http.HttpStatus
import org.springframework.lang.Nullable
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.support.WebDataBinderFactory
import org.springframework.web.context.request.NativeWebRequest
import org.springframework.web.context.request.RequestAttributes
import org.springframework.web.filter.ShallowEtagHeaderFilter
import org.springframework.web.method.support.HandlerMethodArgumentResolver
import org.springframework.web.method.support.ModelAndViewContainer
import org.springframework.web.servlet.HandlerMapping
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer
import org.springframework.web.servlet.config.annotation.InterceptorRegistry
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter

import javax.servlet.Filter
import javax.servlet.http.HttpServletResponse

@Configuration
@ComponentScan([
  'com.netflix.spinnaker.clouddriver.controllers',
  'com.netflix.spinnaker.clouddriver.filters',
  'com.netflix.spinnaker.clouddriver.listeners',
  'com.netflix.spinnaker.clouddriver.security',
])
@EnableConfigurationProperties([CredentialsConfiguration, RequestQueueConfiguration])
public class WebConfig extends WebMvcConfigurerAdapter {
  @Autowired
  Registry registry

  @Autowired
  AccountCredentialsProvider accountCredentialsProvider

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(
      new MetricsInterceptor(
        this.registry, "controller.invocations", ["account", "region"], ["BasicErrorController"]
      )
    )
  }

  @Override
  void addArgumentResolvers(List<HandlerMethodArgumentResolver> argumentResolvers) {
    argumentResolvers.add(
            new HandlerMethodArgumentResolver() {
              @Override
              boolean supportsParameter(MethodParameter parameter) {
                return AccountCredentials == parameter.getParameterType() && parameter.getParameterAnnotation(AccountPathVariable) != null
              }

              @Override
              Object resolveArgument(
                      MethodParameter parameter,
                      @Nullable ModelAndViewContainer mavContainer,
                      NativeWebRequest webRequest,
                      @Nullable WebDataBinderFactory binderFactory)
                      throws Exception {

                String urlParameterName = parameter.getParameterAnnotation(AccountPathVariable).value()

                Map<String, String> uriTemplateVars =
                        (Map<String, String>) webRequest.getAttribute(
                                HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE, RequestAttributes.SCOPE_REQUEST)

                return accountCredentialsProvider.getCredentials(uriTemplateVars.get(urlParameterName))
              }
            }
    )
  }

  @Bean
  Filter eTagFilter() {
    new ShallowEtagHeaderFilter()
  }

  @Bean
  RequestQueue requestQueue(DynamicConfigService dynamicConfigService,
                            RequestQueueConfiguration requestQueueConfiguration,
                            Registry registry) {
    return RequestQueue.forConfig(dynamicConfigService, registry, requestQueueConfiguration);
  }

  @Bean
  FilterRegistrationBean authenticatedRequestFilter() {
    def frb = new FilterRegistrationBean(new AuthenticatedRequestFilter(true))
    frb.order = Ordered.HIGHEST_PRECEDENCE
    return frb
  }

  @Override
  void configureContentNegotiation(ContentNegotiationConfigurer configurer) {
    super.configureContentNegotiation(configurer)
    configurer.favorPathExtension(false)
  }

  @ControllerAdvice
  static class IllegalArgumentExceptionHandler {
    @ExceptionHandler(IllegalArgumentException)
    public void handle(HttpServletResponse response, IllegalArgumentException ex) {
      response.sendError(HttpStatus.BAD_REQUEST.value(), ex.getMessage())
    }
  }
}
