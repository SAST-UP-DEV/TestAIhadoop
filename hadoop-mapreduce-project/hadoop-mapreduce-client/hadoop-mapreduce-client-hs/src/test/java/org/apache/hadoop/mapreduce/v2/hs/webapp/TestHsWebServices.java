/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.StringReader;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import com.google.inject.util.Providers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.mapreduce.v2.hs.JobHistory;
import org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer;
import org.apache.hadoop.mapreduce.v2.hs.MockHistoryContext;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

/**
 * Test the History Server info web services api's. Also test non-existent urls.
 *
 *  /ws/v1/history
 *  /ws/v1/history/info
 */
public class TestHsWebServices extends JerseyTestBase {

  private static Configuration conf = new Configuration();
  private static HistoryContext appContext;
  private static HsWebApp webApp;

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      appContext = new MockHistoryContext(0, 1, 1, 1);
      JobHistory jobHistoryService = new JobHistory();
      HistoryContext historyContext = (HistoryContext) jobHistoryService;
      webApp = new HsWebApp(historyContext);

      bind(JAXBContextResolver.class);
      bind(HsWebServices.class);
      bind(GenericExceptionHandler.class);
      bind(WebApp.class).toInstance(webApp);
      bind(AppContext.class).toInstance(appContext);
      bind(HistoryContext.class).toInstance(appContext);
      bind(Configuration.class).toInstance(conf);
      bind(ApplicationClientProtocol.class).toProvider(Providers.of(null));

      serve("/*").with(GuiceContainer.class);
    }
  }

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  public TestHsWebServices() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.mapreduce.v2.hs.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  void testHS() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    verifyHSInfo(json.getJSONObject("historyInfo"), appContext);
  }

  @Test
  void testHSSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history/")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    verifyHSInfo(json.getJSONObject("historyInfo"), appContext);
  }

  @Test
  void testHSDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history/")
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    verifyHSInfo(json.getJSONObject("historyInfo"), appContext);
  }

  @Test
  void testHSXML() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    String xml = response.getEntity(String.class);
    verifyHSInfoXML(xml, appContext);
  }

  @Test
  void testInfo() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("info").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    verifyHSInfo(json.getJSONObject("historyInfo"), appContext);
  }

  @Test
  void testInfoSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("info/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    verifyHSInfo(json.getJSONObject("historyInfo"), appContext);
  }

  @Test
  void testInfoDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("info/").get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    verifyHSInfo(json.getJSONObject("historyInfo"), appContext);
  }

  @Test
  void testInfoXML() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("info/").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    String xml = response.getEntity(String.class);
    verifyHSInfoXML(xml, appContext);
  }

  @Test
  void testInvalidUri() throws JSONException, Exception {
    WebResource r = resource();
    String responseStr = "";
    try {
      responseStr = r.path("ws").path("v1").path("history").path("bogus")
          .accept(MediaType.APPLICATION_JSON).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  void testInvalidUri2() throws JSONException, Exception {
    WebResource r = resource();
    String responseStr = "";
    try {
      responseStr = r.path("ws").path("v1").path("invalid")
          .accept(MediaType.APPLICATION_JSON).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  void testInvalidAccept() throws JSONException, Exception {
    WebResource r = resource();
    String responseStr = "";
    try {
      responseStr = r.path("ws").path("v1").path("history")
          .accept(MediaType.TEXT_PLAIN).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.INTERNAL_SERVER_ERROR,
          response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  public void verifyHsInfoGeneric(String hadoopVersionBuiltOn,
      String hadoopBuildVersion, String hadoopVersion, long startedon) {
    WebServicesTestUtils.checkStringMatch("hadoopVersionBuiltOn",
        VersionInfo.getDate(), hadoopVersionBuiltOn);
    WebServicesTestUtils.checkStringEqual("hadoopBuildVersion",
        VersionInfo.getBuildVersion(), hadoopBuildVersion);
    WebServicesTestUtils.checkStringMatch("hadoopVersion",
        VersionInfo.getVersion(), hadoopVersion);
    assertEquals(JobHistoryServer.historyServerTimeStamp, startedon, "startedOn doesn't match: ");
  }

  public void verifyHSInfo(JSONObject info, AppContext ctx)
      throws JSONException {
    assertEquals(4, info.length(), "incorrect number of elements");

    verifyHsInfoGeneric(info.getString("hadoopVersionBuiltOn"),
        info.getString("hadoopBuildVersion"), info.getString("hadoopVersion"),
        info.getLong("startedOn"));
  }

  public void verifyHSInfoXML(String xml, AppContext ctx)
      throws JSONException, Exception {
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("historyInfo");
    assertEquals(1, nodes.getLength(), "incorrect number of elements");

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);
      verifyHsInfoGeneric(
          WebServicesTestUtils.getXmlString(element, "hadoopVersionBuiltOn"),
          WebServicesTestUtils.getXmlString(element, "hadoopBuildVersion"),
          WebServicesTestUtils.getXmlString(element, "hadoopVersion"),
          WebServicesTestUtils.getXmlLong(element, "startedOn"));
    }
  }

}
