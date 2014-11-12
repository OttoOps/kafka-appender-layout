package com.twc.needle;

import kafka.producer.KafkaLog4jAppender;
import org.apache.log4j.spi.LoggingEvent;

import java.lang.Boolean;
import java.lang.Override;
import java.lang.String;
import java.util.regex.*;

/**
 * The DruidKafkaLog4jAppender takes metric events from Druid, applies RegEx to them,
 * and sends the transformed events to Kafka.
 *
 * This class extends KafkaLog4jAppender and overrides its subAppend method.
 */
public class DruidKafkaLog4jAppender extends KafkaLog4jAppender {

  /**
   * Source string for client being logged
   */
  protected String clientSource = "druid";

  /**
   * Source version of client being logged
   */
  protected String clientSourceVersion = "0.6.129";

  /**
   * Source cluster name of client being logged
   */
  protected String clientSourceCluster = "Escher";

  /**
   * Debug output to stdout
   */
  protected String appenderDebug = "false";

  /**
   * Message header for checkEntryConditions
   */
  protected String appenderMessageHeader = "Event";

  /**
   * Regex to match against
   */
  protected String appenderRegex = "(Event \\S\\S)(.*)(\"metric\")(.*)(\\S)";

  /**
   * Regex to match against (compiled)
   */
  private Pattern appenderPattern;

  /**
   * Internal instance variable for console debugging
   */
  private boolean isDebug = false;

  /**
   * Constructor
   * Needed to compile the default AppenderRegex
   * Unfortunately, this means that, when provided AppenderRegex from the
   * properties file, pattern compile will occur twice. The Log4j interface
   * does not send parameters to the constructor but instead sends them to
   * the setters.
   */
  public DruidKafkaLog4jAppender() {
    super();
    setAppenderPattern(getAppenderRegex());
  }

  @Override
  public void append(LoggingEvent event) {
    if (!checkEntryConditions(event.getRenderedMessage())) {
      return;
    }
    super.append(event);
  }

  protected boolean checkEntryConditions(String message) {
    String sHeader = this.getAppenderMessageHeader();
    int iHeaderLen = sHeader.length();
    String sMethod = "checkEntryConditions";
    debugOut(message, sMethod, "message");
    debugOut(sHeader, sMethod, "header");
    debugOut(String.valueOf(iHeaderLen), sMethod, "headerlength");
    if (message.contains(sHeader)) {
      if (!message.substring(0,iHeaderLen).equals(sHeader)) {
        debugOut("false", sMethod, "return");
        return false;
      }
    }
    else {
      debugOut("false", sMethod, "return");
      return false;
    }
    debugOut("true", sMethod, "return");
    return true;
  }

  /**
   * Example event (input):
   *  "Event [{"feed":"metrics","timestamp":"2014-07-29T16:23:26.240Z","service":"historical","host":"needle26.lab.mystrotv.com:8081","metric":"sys/swap/pageIn","value":0}]";
   * Example output:
   *  {"source":"druid","sourceVersion":"0.6.129","sourceCluster":"Escher","feed":"metrics","timestamp":"2014-06-10T19:58:04.384Z","service":"middlemanager-rt1","host":"needle13.lab.mystrotv.com:8088","metricName":"jvm/bufferpool/count","value":244,"user2":"mapped"}
   */
  @Override
  public String subAppend(LoggingEvent event) {
    String sMethod = "subAppend";
    String input = event.getRenderedMessage();
    debugOut(input, sMethod, "input");
    Matcher m = this.appenderPattern.matcher(input);
    StringBuffer result = new StringBuffer();
    result.append("{\"source\":\"" + this.getClientSource() + "\",");
    result.append("\"sourceVersion\":\"" + this.getClientSourceVersion() + "\",");
    result.append("\"sourceCluster\":\"" + this.getClientSourceCluster() + "\",");
    while (m.find()) {
      m.appendReplacement(result, m.group(2) + "\"metricName\"" + m.group(4));
    }
    m.appendTail(result);
    String resultString = result.toString();
    debugOut(resultString, sMethod, "resultString");
    return resultString;
  }

  /**
   *  Purpose of debugOut is to proivde console logging without using Log4j
   *  to avoid possibility of stack overflows.
   * @param debugString
   * @param sMethod
   * @param sContext
   */
  private void debugOut(String debugString, String sMethod, String sContext) {
    if (!isDebug) {
      return;
    }
    if (sContext.length() > 0) {
      System.out.println(sMethod + "." + sContext + ": " + debugString);
    } else {
      System.out.println(debugString);
    }
  }

  private void debugOut(String debugString) {
    debugOut(debugString, "", "");
  }

  private void setDebugMode(String sDebug) {
    if (sDebug.equalsIgnoreCase("TRUE") ||
        sDebug.equalsIgnoreCase("YES")) {
      this.isDebug = true;
      debugOut("DruidKafkaLogjAppender Debug ON");
    } else {
      this.isDebug = false;
    }
  }

  private void setAppenderPattern(String appenderRegex) {
    this.appenderPattern = Pattern.compile(appenderRegex);
  }

  public String getClientSource() {
    return clientSource;
  }

  public void setClientSource(String clientSource) {
    this.clientSource = clientSource;
  }

  public String getClientSourceVersion() {
    return clientSourceVersion;
  }

  public void setClientSourceVersion(String clientSourceVersion) {
    this.clientSourceVersion = clientSourceVersion;
  }

  public String getClientSourceCluster() {
    return clientSourceCluster;
  }

  public void setClientSourceCluster(String clientSourceCluster) {
    this.clientSourceCluster = clientSourceCluster;
  }

  public String getAppenderMessageHeader() {
    return appenderMessageHeader;
  }

  public void setAppenderMessageHeader(String appenderMessageHeader) {
    this.appenderMessageHeader = appenderMessageHeader;
  }

  public String getAppenderDebug() {
    return appenderDebug;
  }

  public void setAppenderDebug(String appenderDebug) {
    this.appenderDebug = appenderDebug;
    this.setDebugMode(appenderDebug);
  }

  public String getAppenderRegex() {
    return appenderRegex;
  }

  public void setAppenderRegex(String appenderRegex) {
    this.appenderRegex = appenderRegex;
    setAppenderPattern(appenderRegex);
  }
}
