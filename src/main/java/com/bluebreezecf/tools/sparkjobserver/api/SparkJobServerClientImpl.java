/*
 * Copyright 2014-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bluebreezecf.tools.sparkjobserver.api;


import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

/**
 * The default client implementation of <code>ISparkJobServerClient</code>.
 * With the specific rest api, it can provide abilities to submit and manage 
 * Apache Spark jobs, jars, and job contexts in the Spark Job Server.
 * 
 * @author bluebreezecf
 * @since 2014-09-07
 *
 */
class SparkJobServerClientImpl implements ISparkJobServerClient {
	private static Logger logger = Logger.getLogger(SparkJobServerClientImpl.class);
	private static final int BUFFER_SIZE = 512 * 1024;
	private String jobServerUrl;
	private String userName;
	private String password;
	private ContentType defaultTextWithUtf8 = ContentType.create("text/plain",Consts.UTF_8);
	private ThreadLocal<AtomicInteger> retryTimes = new ThreadLocal<AtomicInteger>();
	private FallbackWithRetryFunction fallbackWithRetry = null;

	/**
	 * Constructs an instance of <code>SparkJobServerClientImpl</code>
	 * with the given spark job server url.
	 * 
	 * @param jobServerUrl a url pointing to a existing spark job server
	 */
	SparkJobServerClientImpl(String jobServerUrl) {
		if (!jobServerUrl.endsWith("/")) {
			jobServerUrl = jobServerUrl + "/";
		}
		this.jobServerUrl = jobServerUrl;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public List<SparkJobJarInfo> getJars() throws SparkJobServerClientException {
		List<SparkJobJarInfo> sparkJobJarInfos = new ArrayList<SparkJobJarInfo>();
		final CloseableHttpClient httpClient = buildClient();
		try {
			HttpGet getMethod = new HttpGet(jobServerUrl + "jars");
			HttpResponse response = httpClient.execute(getMethod);
			int statusCode = response.getStatusLine().getStatusCode();
			String resContent = getResponseContent(response.getEntity());
			if (statusCode == HttpStatus.SC_OK) {
				JSONObject jsonObj = JSONObject.fromObject(resContent);
				Iterator<?> keyIter = jsonObj.keys();
				while (keyIter.hasNext()) {
					String jarName = (String)keyIter.next();
					String uploadedTime = (String)jsonObj.get(jarName);
					SparkJobJarInfo sparkJobJarInfo = new SparkJobJarInfo();
					sparkJobJarInfo.setJarName(jarName);
					sparkJobJarInfo.setUploadedTime(uploadedTime);
					sparkJobJarInfos.add(sparkJobJarInfo);
				}
			} else {
				logError(statusCode, resContent, true);
			}
		} catch (SocketException | UnknownHostException  e){
			int retry = getRetriedTimes();
			Pair<Integer, String> fallback = fallbackWithRetry.getFallback(retry);
			if(retry <= fallback.getValue0()){
				this.jobServerUrl = fallback.getValue1();
				return getJars();
			}else{
				processException("Error occurs when retry to get information of jars, retried "+retry+" times ", e);
			}
		} catch(Exception e) {
			processException("Error occurs when trying to get information of jars:", e);
		} finally {
			resetRetryThreadLocal();
			close(httpClient);
		}
		return sparkJobJarInfos;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean uploadSparkJobJar(InputStream jarData, String appName)
	    throws SparkJobServerClientException {
		if (jarData == null || appName == null || appName.trim().length() == 0) {
			throw new SparkJobServerClientException("Invalid parameters.");
		}
		HttpPost postMethod = new HttpPost(jobServerUrl + "jars/" + appName);

		final CloseableHttpClient httpClient = buildClient();
		try {
			ByteArrayEntity entity = new ByteArrayEntity(IOUtils.toByteArray(jarData));
			postMethod.setEntity(entity);
			entity.setContentType("application/java-archive");
			HttpResponse response = httpClient.execute(postMethod);
			int statusCode = response.getStatusLine().getStatusCode();
			getResponseContent(response.getEntity());
			if (statusCode == HttpStatus.SC_OK) {
				return true;
			}
		}catch (SocketException | UnknownHostException e){
			int retry = getRetriedTimes();
			Pair<Integer, String> fallback = fallbackWithRetry.getFallback(retry);
			if(retry <= fallback.getValue0()){
				this.jobServerUrl = fallback.getValue1();
				return uploadSparkJobJar(jarData, appName);
			}else{
				processException("Error occurs when retry to uploading spark job jars, retried "+retry+" times ", e);
			}
		} catch (Exception e) {
			logger.error("Error occurs when uploading spark job jars:", e);
		} finally {
			resetRetryThreadLocal();
			close(httpClient);
			closeStream(jarData);
		}
		return false;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public boolean uploadSparkJobJar(File jarFile, String appName)
		    throws SparkJobServerClientException {
		if (jarFile == null || !jarFile.getName().endsWith(".jar") 
			|| appName == null || appName.trim().length() == 0) {
			throw new SparkJobServerClientException("Invalid parameters.");
		}
		InputStream jarIn = null;
		try {
			jarIn = new FileInputStream(jarFile);
		} catch (FileNotFoundException fnfe) {
			String errorMsg = "Error occurs when getting stream of the given jar file";
			logger.error(errorMsg, fnfe);
			throw new SparkJobServerClientException(errorMsg, fnfe);
		}
		return uploadSparkJobJar(jarIn, appName);
	}

	/**
	 * {@inheritDoc}
	 */
	public List<String> getContexts() throws SparkJobServerClientException {
		List<String> contexts = new ArrayList<String>();
		final CloseableHttpClient httpClient = buildClient();
		try {
			HttpGet getMethod = new HttpGet(jobServerUrl + "contexts");
			HttpResponse response = httpClient.execute(getMethod);
			int statusCode = response.getStatusLine().getStatusCode();
			String resContent = getResponseContent(response.getEntity());
			if (statusCode == HttpStatus.SC_OK) {
				JSONArray jsonArray = JSONArray.fromObject(resContent);
				Iterator<?> iter = jsonArray.iterator();
				while (iter.hasNext()) {
					contexts.add((String)iter.next());
				}
			} else {
				logError(statusCode, resContent, true);
			}
		} catch (SocketException | UnknownHostException e){
			int retry = getRetriedTimes();
			Pair<Integer, String> fallback = fallbackWithRetry.getFallback(retry);
			if(retry <= fallback.getValue0()){
				this.jobServerUrl = fallback.getValue1();
				return getContexts();
			}else{
				processException("Error occurs when retry to get information of contexts, retried "+retry+" times ", e);
			}
		}catch (Exception e) {
			processException("Error occurs when trying to get information of contexts:", e);
		} finally {
			resetRetryThreadLocal();
			close(httpClient);
		}
		return contexts;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean createContext(String contextName, Map<String, String> params) 
		throws SparkJobServerClientException {
		final CloseableHttpClient httpClient = buildClient();
		try {
			//TODO add a check for the validation of contextName naming
			if (!isNotEmpty(contextName)) {
				throw new SparkJobServerClientException("The given contextName is null or empty.");
			}
			StringBuffer postUrlBuff = new StringBuffer(jobServerUrl);
			postUrlBuff.append("contexts/").append(contextName);
			if (params != null && !params.isEmpty()) {
				postUrlBuff.append('?');
				int num = params.size();
				for (String key : params.keySet()) {
					postUrlBuff.append(key).append('=').append(params.get(key));
					num--;
					if (num > 0) {
						postUrlBuff.append('&');
					}
				}
				
			}
			HttpPost postMethod = new HttpPost(postUrlBuff.toString());
			HttpResponse response = httpClient.execute(postMethod);
			int statusCode = response.getStatusLine().getStatusCode();
			String resContent = getResponseContent(response.getEntity());
			if (statusCode == HttpStatus.SC_OK) {
				return true;
			} else {
				logError(statusCode, resContent, false);
			}
		} catch (SocketException | UnknownHostException e){
			int retry = getRetriedTimes();
			Pair<Integer, String> fallback = fallbackWithRetry.getFallback(retry);
			if(retry <= fallback.getValue0()){
				this.jobServerUrl = fallback.getValue1();
				return createContext(contextName, params);
			}else{
				processException("Error occurs when retry to create a context, retried "+retry+" times ", e);
			}
		} catch (Exception e) {
			processException("Error occurs when trying to create a context:", e);
		} finally {
			resetRetryThreadLocal();
			close(httpClient);
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean deleteContext(String contextName) 
		throws SparkJobServerClientException {
		final CloseableHttpClient httpClient = buildClient();
		try {
			//TODO add a check for the validation of contextName naming
			if (!isNotEmpty(contextName)) {
				throw new SparkJobServerClientException("The given contextName is null or empty.");
			}
			StringBuffer postUrlBuff = new StringBuffer(jobServerUrl);
			postUrlBuff.append("contexts/").append(contextName);
			
			HttpDelete deleteMethod = new HttpDelete(postUrlBuff.toString());
			HttpResponse response = httpClient.execute(deleteMethod);
			int statusCode = response.getStatusLine().getStatusCode();
			String resContent = getResponseContent(response.getEntity());
			if (statusCode == HttpStatus.SC_OK) {
				return true;
			} else {
				logError(statusCode, resContent, false);
			}
		} catch (SocketException | UnknownHostException e){
			int retry = getRetriedTimes();
			Pair<Integer, String> fallback = fallbackWithRetry.getFallback(retry);
			if(retry <= fallback.getValue0()){
				this.jobServerUrl = fallback.getValue1();
				return deleteContext(contextName);
			}else{
				processException("Error occurs when retry to delete the target context, retried "+retry+" times ", e);
			}
		} catch (Exception e) {
			processException("Error occurs when trying to delete the target context:", e);
		} finally {
			resetRetryThreadLocal();
			close(httpClient);
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	public List<SparkJobInfo> getJobs() throws SparkJobServerClientException {
		List<SparkJobInfo> sparkJobInfos = new ArrayList<SparkJobInfo>();
		final CloseableHttpClient httpClient = buildClient();
		try {
			HttpGet getMethod = new HttpGet(jobServerUrl + "jobs");
			HttpResponse response = httpClient.execute(getMethod);
			int statusCode = response.getStatusLine().getStatusCode();
			String resContent = getResponseContent(response.getEntity());
			if (statusCode == HttpStatus.SC_OK) {
				JSONArray jsonArray = JSONArray.fromObject(resContent);
				Iterator<?> iter = jsonArray.iterator();
				while (iter.hasNext()) {
					JSONObject jsonObj = (JSONObject)iter.next();
					SparkJobInfo jobInfo = new SparkJobInfo();
					jobInfo.setDuration(jsonObj.getString(SparkJobInfo.INFO_KEY_DURATION));
					jobInfo.setClassPath(jsonObj.getString(SparkJobInfo.INFO_KEY_CLASSPATH));
					jobInfo.setStartTime(jsonObj.getString(SparkJobInfo.INFO_KEY_START_TIME));
					jobInfo.setContext(jsonObj.getString(SparkJobBaseInfo.INFO_KEY_CONTEXT));
					jobInfo.setStatus(jsonObj.getString(SparkJobBaseInfo.INFO_KEY_STATUS));
					jobInfo.setJobId(jsonObj.getString(SparkJobBaseInfo.INFO_KEY_JOB_ID));
					setErrorDetails(SparkJobBaseInfo.INFO_KEY_RESULT, jsonObj, jobInfo);
					sparkJobInfos.add(jobInfo);
				}
			} else {
				logError(statusCode, resContent, true);
			}
		} catch (SocketException | UnknownHostException e){
			int retry = getRetriedTimes();
			Pair<Integer, String> fallback = fallbackWithRetry.getFallback(retry);
			if(retry <= fallback.getValue0()){
				return getJobs();
			}else{
				processException("Error occurs when retry to get information of jobs, retried "+retry+" times ", e);
			}
		} catch (Exception e) {
			processException("Error occurs when trying to get information of jobs:", e);
		} finally {
			resetRetryThreadLocal();
			close(httpClient);
		}
		return sparkJobInfos;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public SparkJobResult startJob(String data, Map<String, String> params) throws SparkJobServerClientException {
		final CloseableHttpClient httpClient = buildClient();
		try {
			if (params == null || params.isEmpty()) {
				throw new SparkJobServerClientException("The given params is null or empty.");
			}
			if (params.containsKey(ISparkJobServerClientConstants.PARAM_APP_NAME) &&
			    params.containsKey(ISparkJobServerClientConstants.PARAM_CLASS_PATH)) {
				StringBuffer postUrlBuff = new StringBuffer(jobServerUrl);
				postUrlBuff.append("jobs?");
				int num = params.size();
				for (String key : params.keySet()) {
					postUrlBuff.append(key).append('=').append(params.get(key));
					num--;
					if (num > 0) {
						postUrlBuff.append('&');
					}
				}
				HttpPost postMethod = new HttpPost(postUrlBuff.toString());
				if (data != null) {
					StringEntity strEntity = new StringEntity(data, defaultTextWithUtf8);
					strEntity.setContentEncoding("UTF-8");
					strEntity.setContentType("text/plain;charset=utf-8");
					postMethod.setEntity(strEntity);
				}
				
				HttpResponse response = httpClient.execute(postMethod);
				String resContent = getResponseContent(response.getEntity());
				int statusCode = response.getStatusLine().getStatusCode();
				if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_ACCEPTED) {
					return parseResult(resContent);
				} else {
					logError(statusCode, resContent, true);
				}
			} else {
				throw new SparkJobServerClientException("The given params should contains appName and classPath");
			}
		} catch (SocketException | UnknownHostException e){
			int retry = getRetriedTimes();
			Pair<Integer, String> fallback = fallbackWithRetry.getFallback(retry);
			if(retry <= fallback.getValue0()){
				this.jobServerUrl = fallback.getValue1();
				return startJob(data, params);
			}else{
				processException("Error occurs when retry to start a new job, retried "+retry+" times ", e);
			}
		} catch (Exception e) {
			processException("Error occurs when trying to start a new job:", e);
		} finally {
			resetRetryThreadLocal();
			close(httpClient);
		}
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	public SparkJobResult startJob(InputStream dataFileStream, Map<String, String> params) throws SparkJobServerClientException {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(dataFileStream));
			String data = br.lines().collect(Collectors.joining(System.lineSeparator()));
			return startJob(data, params);
		} catch (Exception e) {
			processException("Error occurs when reading inputstream:", e);
		} finally {
			closeStream(br);
		}
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	public SparkJobResult startJob(File dataFile, Map<String, String> params) throws SparkJobServerClientException {
		InputStream dataFileStream = null;
		try {
			dataFileStream = new FileInputStream(dataFile);
			return startJob(dataFileStream, params);
		} catch (Exception e) {
			processException("Error occurs when reading file:", e);
		} finally {
			closeStream(dataFileStream);
		}
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	public SparkJobResult getJobResult(String jobId) throws SparkJobServerClientException {
		final CloseableHttpClient httpClient = buildClient();
		try {
			if (!isNotEmpty(jobId)) {
				throw new SparkJobServerClientException("The given jobId is null or empty.");
			}
			HttpGet getMethod = new HttpGet(jobServerUrl + "jobs/" + jobId);
			HttpResponse response = httpClient.execute(getMethod);
			String resContent = getResponseContent(response.getEntity());
			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode == HttpStatus.SC_OK) {
				final SparkJobResult jobResult = parseResult(resContent);
				jobResult.setJobId(jobId);
				return jobResult;
			} else if (statusCode == HttpStatus.SC_NOT_FOUND) {
				return new SparkJobResult(resContent, jobId);
			} else {
				logError(statusCode, resContent, true);
			}
		} catch (SocketException | UnknownHostException e){
			int retry = getRetriedTimes();
			Pair<Integer, String> fallback = fallbackWithRetry.getFallback(retry);
			if(retry <= fallback.getValue0()){
				this.jobServerUrl = fallback.getValue1();
				return getJobResult(jobId);
			}else{
				processException("Error occurs when retry to get information of the target job, retried "+retry+" times ", e);
			}
		} catch (Exception e) {
			processException("Error occurs when trying to get information of the target job:", e);
		} finally {
			resetRetryThreadLocal();
			close(httpClient);
		}
		return null;
	}


	public boolean deleteJob(String jobId) throws SparkJobServerClientException {
		final CloseableHttpClient httpClient = buildClient();
		try {
			if (!isNotEmpty(jobId)) {
				throw new SparkJobServerClientException("The given jobId is null or empty.");
			}
			StringBuffer postUrlBuff = new StringBuffer(jobServerUrl);
			postUrlBuff.append("jobs/").append(jobId);
			HttpDelete deleteMethod = new HttpDelete(postUrlBuff.toString());
			HttpResponse response = httpClient.execute(deleteMethod);
			int statusCode = response.getStatusLine().getStatusCode();
			String resContent = getResponseContent(response.getEntity());
			if (statusCode == HttpStatus.SC_OK) {
				return true;
			} else {
				logError(statusCode, resContent, false);
			}
		} catch (SocketException | UnknownHostException e){
			int retry = getRetriedTimes();
			Pair<Integer, String> fallback = fallbackWithRetry.getFallback(retry);
			if(retry <= fallback.getValue0()){
				this.jobServerUrl = fallback.getValue1();
				return deleteJob(jobId);
			}else{
				processException("Error occurs when retry to delete the target context, retried "+retry+" times ", e);
			}
		} catch (Exception e) {
			processException("Error occurs when trying to delete the target context:", e);
		} finally {
			resetRetryThreadLocal();
			close(httpClient);
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	public SparkJobConfig getConfig(String jobId) throws SparkJobServerClientException {
		final CloseableHttpClient httpClient = buildClient();
		try {
			if (!isNotEmpty(jobId)) {
				throw new SparkJobServerClientException("The given jobId is null or empty.");
			}
			HttpGet getMethod = new HttpGet(jobServerUrl + "jobs/" + jobId + "/config");
			HttpResponse response = httpClient.execute(getMethod);
			String resContent = getResponseContent(response.getEntity());
			JSONObject jsonObj = JSONObject.fromObject(resContent);
			SparkJobConfig jobConfg = new SparkJobConfig();
			Iterator<?> keyIter = jsonObj.keys();
			while (keyIter.hasNext()) {
				String key = (String)keyIter.next();
				jobConfg.putConfigItem(key, jsonObj.get(key));
			}
			return jobConfg;
		} catch (SocketException | UnknownHostException e){
			int retry = getRetriedTimes();
			Pair<Integer, String> fallback = fallbackWithRetry.getFallback(retry);
			if(retry <= fallback.getValue0()){
				this.jobServerUrl = fallback.getValue1();
				return getConfig(jobId);
			}else{
				processException("Error occurs when retry to get information of the target job config, retried "+retry+" times ", e);
			}
		} catch (Exception e) {
			processException("Error occurs when trying to get information of the target job config:", e);
		} finally {
			resetRetryThreadLocal();
			close(httpClient);
		}
		return null;
	}


	@Override
	public void initialize() throws SparkJobServerClientException {

	}

	private void resetRetryThreadLocal() {
		if(retryTimes.get() == null){
			retryTimes.set(new AtomicInteger(0));
		}
		retryTimes.get().set(0);
	}


	/**
	 * Gets the contents of the http response from the given <code>HttpEntity</code>
	 * instance.
	 * 
	 * @param entity the <code>HttpEntity</code> instance holding the http response content
	 * @return the corresponding response content
	 */
	protected String getResponseContent(HttpEntity entity) {
		byte[] buff = new byte[BUFFER_SIZE];
		StringBuffer contents = new StringBuffer();
		InputStream in = null;
		try {
			String encoding = "utf-8";
//			StringUtils.substring(entity.getContentType().getValue(), entity.getContentType().getValue().indexOf("charset") + 8);
			in = entity.getContent();
			BufferedInputStream bis = new BufferedInputStream(in);
			int readBytes = 0;
			while ((readBytes = bis.read(buff)) != -1) {
				contents.append(new String(buff, 0, readBytes, encoding));
			}
		} catch (Exception e) {
			logger.error("Error occurs when trying to reading response", e);
		} finally {
			closeStream(in);
		}
		return contents.toString().trim();
	}
	
	/**
	 * Closes the given stream.
	 * 
	 * @param stream the input/output stream to be closed
	 */
	protected void closeStream(Closeable stream) {
		if (stream != null) {
			try {
				stream.close();
			} catch (IOException ioe) {
				logger.error("Error occurs when trying to close the stream:", ioe);
			}
		} else {
			logger.error("The given stream is null");
		}
	}
	
	/**
	 * Handles the given exception with specific error message, and
	 * generates a corresponding <code>SparkJobServerClientException</code>.
	 * 
	 * @param errorMsg the corresponding error message
	 * @param e the exception to be handled
	 * @throws SparkJobServerClientException the corresponding transformed 
	 *        <code>SparkJobServerClientException</code> instance
	 */
	protected void processException(String errorMsg, Exception e) throws SparkJobServerClientException {
		if (e instanceof SparkJobServerClientException) {
			throw (SparkJobServerClientException)e;
		}
		logger.error(errorMsg, e);
		throw new SparkJobServerClientException(errorMsg, e);
	}
	
	/**
	 * Judges the given string value is not empty or not.
	 * 
	 * @param value the string value to be checked
	 * @return true indicates it is not empty, false otherwise
	 */
	protected boolean isNotEmpty(String value) {
		return value != null && !value.isEmpty();
	}
	
	/**
	 * Logs the response information when the status is not 200 OK,
	 * and throws an instance of <code>SparkJobServerClientException<code>.
	 * 
	 * @param errorStatusCode error status code
	 * @param msg the message to indicates the status, it can be null
	 * @param throwable true indicates throws an instance of <code>SparkJobServerClientException</code>
	 *       with corresponding error message, false means only log the error message.
	 * @throws SparkJobServerClientException containing the corresponding error message 
	 */
	private void logError(int errorStatusCode, String msg, boolean throwable) throws SparkJobServerClientException {
		StringBuffer msgBuff = new StringBuffer("Spark Job Server ");
		msgBuff.append(jobServerUrl).append(" response ").append(errorStatusCode);
		if (null != msg) {
			msgBuff.append(" ").append(msg);
		}
		String errorMsg = msgBuff.toString();
		logger.error(errorMsg);
		if (throwable) {
			throw new SparkJobServerClientException(errorMsg);
		}
	}
	
	/**
	 * Sets the information of the error details.
	 * 
	 * @param key the key contains the error details
	 * @param parnetJsonObj the parent <code>JSONObject</code> instance
	 */
	private void setErrorDetails(String key, JSONObject parnetJsonObj, SparkJobBaseInfo jobErrorInfo) {
		if (parnetJsonObj.containsKey(key)) {
			JSONObject resultJson = parnetJsonObj.getJSONObject(key);
			if (resultJson.containsKey(SparkJobInfo.INFO_KEY_RESULT_MESSAGE)) {
				jobErrorInfo.setMessage(resultJson.getString(SparkJobInfo.INFO_KEY_RESULT_MESSAGE));
			}
			if (resultJson.containsKey(SparkJobInfo.INFO_KEY_RESULT_ERROR_CLASS)) {
				jobErrorInfo.setErrorClass(resultJson.getString(SparkJobInfo.INFO_KEY_RESULT_ERROR_CLASS));
			}
			if (resultJson.containsKey(SparkJobInfo.INFO_KEY_RESULT_STACK)) {
				JSONArray stackJsonArray = null;
                try {
                    stackJsonArray = resultJson.getJSONArray(SparkJobInfo.INFO_KEY_RESULT_STACK);
                }catch (Exception e){
                    // nothing
                }
                if(stackJsonArray != null){
                    String[] stack = new String[stackJsonArray.size()];
                    for (int i = 0; i < stackJsonArray.size(); i++) {
                        stack[i] = stackJsonArray.getString(i);
                    }
                    jobErrorInfo.setStack(stack);
                }else {
                    String stack0 = resultJson.getString(SparkJobInfo.INFO_KEY_RESULT_STACK);
                    jobErrorInfo.setStack(new String[]{stack0});
                }
			}
		}
	}
	
	/**
	 * Generates an instance of <code>SparkJobResult</code> according to the given contents.
	 * 
	 * @param resContent the content of a http response
	 * @return the corresponding <code>SparkJobResult</code> instance
	 * @throws Exception error occurs when parsing the http response content
	 */
	private SparkJobResult parseResult(String resContent) throws Exception {
		JSONObject jsonObj = JSONObject.fromObject(resContent);
		SparkJobResult jobResult = new SparkJobResult(resContent);
		boolean completed = false;
		if(jsonObj.has(SparkJobBaseInfo.INFO_KEY_STATUS)) {
			jobResult.setStatus(jsonObj.getString(SparkJobBaseInfo.INFO_KEY_STATUS));
			if (SparkJobBaseInfo.COMPLETED.contains(jobResult.getStatus())) {
				completed = true;
			}
		} else {
			completed = true;
		}
		if (completed && jsonObj.containsKey(SparkJobBaseInfo.INFO_KEY_RESULT)) {
			//Job finished with results
			jobResult.setResult(jsonObj.get(SparkJobBaseInfo.INFO_KEY_RESULT).toString());
		} else if (containsAsynjobStatus(jsonObj)) {
			//asynchronously started job only with status information
			setAsynjobStatus(jobResult, jsonObj);
		} else if (containsErrorInfo(jsonObj)) {
			String errorKey = null;
			if (jsonObj.containsKey(SparkJobBaseInfo.INFO_STATUS_ERROR)) {
				errorKey = SparkJobBaseInfo.INFO_STATUS_ERROR;
			} else if (jsonObj.containsKey(SparkJobBaseInfo.INFO_KEY_RESULT)) {
				errorKey = SparkJobBaseInfo.INFO_KEY_RESULT;
			} 
			//Job failed with error details
			setErrorDetails(errorKey, jsonObj, jobResult);
		} else {
			//Other unknown kind of value needs application to parse itself
			Iterator<?> keyIter = jsonObj.keys();
			while (keyIter.hasNext()) {
				String key = (String)keyIter.next();
				if (SparkJobInfo.INFO_KEY_STATUS.equals(key)) {
					continue;
				}
				jobResult.putExtendAttribute(key, jsonObj.get(key));
			}
		}
		return jobResult;
	}
	
	/**
	 * Judges the given json object contains the error information of a  
	 * spark job or not.
	 * 
	 * @param jsonObj the <code>JSONObject</code> instance to be checked.
	 * @return true if it contains the error information, false otherwise
	 */
	private boolean containsErrorInfo(JSONObject jsonObj) {
		return SparkJobBaseInfo.INFO_STATUS_ERROR.equals(jsonObj.getString(SparkJobBaseInfo.INFO_KEY_STATUS));
	}
	
	
	/**
	 * Judges the given json object contains the status information of a asynchronous 
	 * started spark job or not.
	 * 
	 * @param jsonObj the <code>JSONObject</code> instance to be checked.
	 * @return true if it contains the status information of a asynchronous 
	 *         started spark job, false otherwise
	 */
	private boolean containsAsynjobStatus(JSONObject jsonObj) {
		return jsonObj != null && jsonObj.containsKey(SparkJobBaseInfo.INFO_KEY_STATUS)
		    && SparkJobBaseInfo.INFO_STATUS_STARTED.equals(jsonObj.getString(SparkJobBaseInfo.INFO_KEY_STATUS))
		    && jsonObj.containsKey(SparkJobBaseInfo.INFO_KEY_RESULT);
	}
	
	/**
	 * Sets the status information of a asynchronous started spark job to the given
	 * job result instance.
	 * 
	 * @param jobResult the <code>SparkJobResult</code> instance to be set the status information
	 * @param jsonObj the <code>JSONObject</code> instance holds the status information
	 */
	private void setAsynjobStatus(SparkJobResult jobResult, JSONObject jsonObj) {
		JSONObject resultJsonObj = jsonObj.getJSONObject(SparkJobBaseInfo.INFO_KEY_RESULT);
		jobResult.setContext(resultJsonObj.getString(SparkJobBaseInfo.INFO_KEY_CONTEXT));
		jobResult.setJobId(resultJsonObj.getString(SparkJobBaseInfo.INFO_KEY_JOB_ID));
	}

	private CloseableHttpClient buildClient() {
		HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
		if(StringUtils.isNotBlank(this.userName) && StringUtils.isNotBlank(this.password)){
			BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
			UsernamePasswordCredentials creds = new UsernamePasswordCredentials(userName, password);
			basicCredentialsProvider.setCredentials(AuthScope.ANY, creds);
			httpClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider);
		}
		return httpClientBuilder.build();
	}

	private void close(final CloseableHttpClient client) {
		try {
			client.close();
		} catch (final IOException e) {
			logger.error("could not close client" , e);
		}
	}

	/**
	 * @return
	 */
	private Integer getRetriedTimes(){
		if(retryTimes.get() == null){
			retryTimes.set(new AtomicInteger(0));
		}
		return retryTimes.get().incrementAndGet();
	}

	public void setCredentials(String userName, String password) {
        this.userName = userName;
        this.password = password;
	}

	public void setFallbackWithRetry(FallbackWithRetryFunction fallbackWithRetry) {
		this.fallbackWithRetry = fallbackWithRetry;
	}
}
