package com.bluebreezecf.tools.sparkjobserver.api;

import com.netflix.ribbon.ClientOptions;
import com.netflix.ribbon.Ribbon;
import com.netflix.ribbon.http.HttpResourceGroup;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import io.reactivex.netty.protocol.http.client.*;
import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class SparkJobServerClientRibbonImpl implements ISparkJobServerClient  {
    private String username;
    private String password;
    private String clientName;
    private String[] listServers;
    private HttpResourceGroup httpClient;

    /**
     *
     * @param clientName
     * @param servers
     */
    public SparkJobServerClientRibbonImpl(String clientName, String[] servers){
       this.clientName = clientName;
       this.listServers = servers;
       this.httpClient = buildClient();
    }


    @Override
    public List<SparkJobJarInfo> getJars() throws SparkJobServerClientException {
        return null;
    }

    @Override
    public boolean uploadSparkJobJar(InputStream jarData, String appName) throws SparkJobServerClientException {
        return false;
    }

    @Override
    public boolean uploadSparkJobJar(File jarFile, String appName) throws SparkJobServerClientException {
        return false;
    }

    @Override
    public List<String> getContexts() throws SparkJobServerClientException {
        return null;
    }

    @Override
    public boolean createContext(String contextName, Map<String, String> params) throws SparkJobServerClientException {
        return false;
    }

    @Override
    public boolean deleteContext(String contextName) throws SparkJobServerClientException {
        return false;
    }

    @Override
    public List<SparkJobInfo> getJobs() throws SparkJobServerClientException {
        return null;
    }

    @Override
    public SparkJobResult startJob(String data, Map<String, String> params) throws SparkJobServerClientException {
        return null;
    }

    @Override
    public SparkJobResult startJob(File dataFile, Map<String, String> params) throws SparkJobServerClientException {
        return null;
    }

    @Override
    public SparkJobResult startJob(InputStream dataFileStream, Map<String, String> params) throws SparkJobServerClientException {
        return null;
    }

    @Override
    public SparkJobResult getJobResult(String jobId) throws SparkJobServerClientException {
        return null;
    }

    @Override
    public SparkJobConfig getConfig(String jobId) throws SparkJobServerClientException {
        return null;
    }

    @Override
    public boolean deleteJob(String jobId) throws SparkJobServerClientException {
        return false;
    }

    /**
     * @return
     */
    private HttpResourceGroup buildClient() {
        HttpResourceGroup.Builder builder = Ribbon.createHttpResourceGroupBuilder(clientName);
        ClientOptions clientOptions = ClientOptions.create()
                .withMaxAutoRetriesNextServer(3)
                .withConfigurationBasedServerList(StringUtils.join(listServers, ","));

        builder = builder.withClientOptions(clientOptions);
        if(StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)){
            String userpwdBase64 = Base64.encodeBase64String((username+":"+password).getBytes());
            builder = builder.withHeader("Authorization", "Basic "+userpwdBase64);
        }

        HttpResourceGroup httpResourceGroup = builder.build();
        return httpResourceGroup;
    }

    /**
     * @param username
     */
    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
