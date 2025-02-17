package com.ekkelenkamp.netatmo2wow;

import com.ekkelenkamp.netatmo2wow.model.Measures;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

/**
 * Documentation from WOW website:
 * <p/>
 * Mandatory Information:
 * All uploads must contain 4 pieces of mandatory information plus at least 1 piece of weather data.
 * Site ID - siteid:
 * The unique numeric id of the site
 * AWS Pin - siteAuthenticationKey:
 * A pin number, chosen by the user to authenticate with WOW.
 * Date - dateutc:
 * Each observation must have a date, in the date encoding specified below.
 * Software Type - softwaretype
 * The name of the software, to identify which piece of software and which version is uploading data
 * <p/>
 * http://wow.metoffice.gov.uk/automaticreading?siteid=123456&siteAuthenticationKey=654321&dateutc=2011-02-02+10%3A32%3A55&winddir=230&windspeedmph=12&windgustmph=12& windgustdir=25&humidity=90&dewptf=68.2&tempf=70&rainin=0&dailyrainin=5&baromin=29.1&soiltempf=25&soilmoisture=25&visibility=25&softwaretype=weathersoftware1.0
 */

public class WowUpload {

    public static final String WOW_URL = "http://wow.metoffice.gov.uk/automaticreading?";

    private static final Logger log = LogManager.getLogger(WowUpload.class);

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'%20'HH'%3A'mm'%3A'ss");

    private int connectionTimeout = 60000;
    private int readTimeout = 60000;
    private long previousTimeStep;
    private String softwareType = Info.SOFTWARE_NAME + " " + Info.SOFTWARE_VERSION;

    public WowUpload(long previousTimeStep) {
        this.previousTimeStep = previousTimeStep;

    }

    /**
     * return timestep of lates upload.
     *
     * @param measures
     * @param siteId
     * @param awsPin
     * @return
     * @throws IOException
     */
    public long upload(List<Measures> measures, final String siteId, final int awsPin) throws IOException {
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        long lastUpload = previousTimeStep;
        int numberOfSuccesfulUploads = 0;
        
        if (!measures.isEmpty())
        {        
	        Double accumuledRain = measures.get(measures.size()-1).getRainAccumulated();
	        for (Measures measure : measures) 
	        {
	            if (measure.getTimestamp() <= previousTimeStep || measure.getTemperature() == null)
	                continue; // was already uploaded.
	            
	            if (measure.getRainAccumulated() == null)
	            {
	            	measure.setRainAccumulated(accumuledRain);
	            }
	            
	            HttpURLConnection connection = getHttpURLConnection(new URL(WOW_URL));
	            try 
	            {
	                setRequestParameters(connection, siteId, awsPin, softwareType, measure);
	                log.debug("Start execution of WOW upload. URL={}", connection);
	                connection.connect();
	                int responseCode = connection.getResponseCode();
	                if (responseCode == HttpURLConnection.HTTP_OK) 
	                {
	                    log.debug("Successfully uploaded data for siteId {}.", siteId);
	                    numberOfSuccesfulUploads++;
	                    if (measure.getTimestamp() > lastUpload)
	                    {
	                        lastUpload = measure.getTimestamp();
	                    }
	                } 
	                else 
	                {
	                    log.warn("Invalid response code {}.", responseCode);
	                }
	            } 
	            finally 
	            {
	                connection.disconnect();
	            }
	        }
        }
        
        log.info("Number of new WOW measurements uploaded: {}", numberOfSuccesfulUploads);
        return lastUpload;
    }

    private static void setRequestParameters(HttpURLConnection connection, String siteId, int awsPin, String softwareType, Measures measure) throws IOException {

        StringBuilder requestBuilder = new StringBuilder(10);
        String urlString = connection.getURL().toString();
        if (!urlString.endsWith("?")) {
            requestBuilder.append('?');
        }
        requestBuilder.append("siteid=");
        requestBuilder.append(siteId);
        requestBuilder.append('&');
        requestBuilder.append("siteAuthenticationKey=");
        requestBuilder.append(URLEncoder.encode(Integer.toString(awsPin), StandardCharsets.UTF_8));
        requestBuilder.append('&');
        requestBuilder.append("softwaretype=");
        requestBuilder.append(URLEncoder.encode(softwareType, StandardCharsets.UTF_8));
        
        for (String parameter : measure.getWowParameters().keySet()) 
        {
            requestBuilder.append('&');
            requestBuilder.append(parameter);
            requestBuilder.append('=');
            requestBuilder.append(URLEncoder.encode(measure.getWowParameters().get(parameter), StandardCharsets.UTF_8));
        }
        String parameterString = requestBuilder.toString();
        log.debug("Executing URL command: {}{}", urlString, parameterString);
        try (OutputStream outputStream = connection.getOutputStream();
        		BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));) {
            bufferedWriter.write(parameterString);
            bufferedWriter.flush();
        }
    }

    private HttpURLConnection getHttpURLConnection(URL url) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setConnectTimeout(connectionTimeout);
        connection.setReadTimeout(readTimeout);
        connection.setDoOutput(true);
        connection.setDoInput(true);
        return connection;
    }
}
