package com.ekkelenkamp.netatmo2wow;

import com.ekkelenkamp.netatmo2wow.model.Device;
import com.ekkelenkamp.netatmo2wow.model.Measures;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.net.URL;
import java.util.*;
import java.util.Map.Entry;

public class NetatmoDownload {
	private NetatmoHttpClient netatmoHttpClient;
	private NetatmoTokenFiles netatmoTokenFiles;
	private String clientId;
	private String clientSecret;

	private static final String REFRESH_TOKEN = "refresh_token";
	private static final String ACCESS_TOKEN = "access_token";
    static final Logger logger = LogManager.getLogger(NetatmoDownload.class);
    static final long TIME_STEP_TOLERANCE = 2L * 60L * 1000L;

    // API URLs that will be used for requests, see: http://dev.netatmo.com/doc/restapi.
    protected static final String URL_BASE = "https://api.netatmo.net";
    protected static final String URL_REQUEST_TOKEN = URL_BASE + "/oauth2/token";
    protected static final String URL_GET_MEASURES_LIST = URL_BASE + "/api/getmeasure";
    protected static final String URL_GET_STATION_DATA = URL_BASE + "/api/getstationsdata";

    public NetatmoDownload(NetatmoHttpClient netatmoHttpClient, NetatmoTokenFiles netatmoTokenFiles) {
        this.netatmoHttpClient = netatmoHttpClient;
        this.netatmoTokenFiles = netatmoTokenFiles;
    }

    public List<Measures> downloadMeasures(String clientId, String clientSecret, String timespan) {
    	this.clientId = clientId;
    	this.clientSecret = clientSecret;
        
        String scale = "max";
        long timePeriod = Long.parseLong(timespan);
        
        // netatmo calculates in seconds, not milliseconds.
        long currentDate = ((new java.util.Date().getTime()) / 1000) - timePeriod;
        logger.debug("start time: {}", new Date(currentDate * 1000));
        logger.debug("start time seconds: {}", currentDate);
        
        Device device = getDevicesAndRefreskTokenIfNeeded(netatmoTokenFiles.readToken(NetatmoTokenType.ACCESS));
        List<Measures> measures = new ArrayList<>();       
        Map<String, List<String>> devices = device.getDevices();
        
        String accessToken = netatmoTokenFiles.readToken(NetatmoTokenType.ACCESS);
    	logger.debug("Access Token: {}", accessToken);
        
    	Double accumulatedRain = 0.0;
        for (Entry<String, List<String>> dev : devices.entrySet()) 
        {
        	measures.addAll(getMeasures(accessToken, dev.getKey(), null, "Pressure" , scale, currentDate, ""));
            List<String> modules = dev.getValue();
            
            for (String module : modules) 
            {
                logger.debug("Device: {}", device);
                logger.debug("Module: {}", module);

                String moduleMeasureTypes = device.getModuleDataType(module);
                
                if (moduleMeasureTypes.equals("Rain"))
                {
                    List<Measures> accumRain = 
                    		getMeasures(accessToken, dev.getKey(), module, "sum_rain", "1day", currentDate, "last");
                    
                    if (!accumRain.isEmpty())
                    {
                    	accumulatedRain = accumRain.get(0).getRainAccumulated();
                    }
                }

                List<Measures> newMeasures = getMeasures(accessToken, dev.getKey(), module, moduleMeasureTypes, scale, currentDate, "");
                measures = mergeMeasures(measures, newMeasures, TIME_STEP_TOLERANCE);
            }
        }
        
        Collections.sort(measures);
        calculateAccumulativeRainfail(measures);
        
        if (!measures.isEmpty())
        {
        	measures.get(measures.size() - 1).setRainAccumulated(accumulatedRain);
        }
        
        return measures;
    }

    private void calculateAccumulativeRainfail(List<Measures> measures) 
    {
        for (int i = measures.size() - 1; i > 0; i--) 
        {
            Measures latestMeasure = measures.get(i);
            // now get all measures before and including this one until we accumulated 1 hour of rainfall.
            Long start = latestMeasure.getTimestamp();
            Long hourDif = (long) 1000 * 60 * 60; // 1 hour.
            Double accumulatedRainfall = 0.0;
            for (int j = i; j > 0 && latestMeasure.getRain() != null; j--) 
            {
                Measures currentMeasure = measures.get(j);
                if (start - currentMeasure.getTimestamp() < hourDif) 
                {
                    // no hour passed yet.
                    accumulatedRainfall += currentMeasure.getRain();
                } 
                else 
                {
                    latestMeasure.setRainLastHour(accumulatedRainfall);
                    break;
                }
            }
        }
    }

    /**
     * Merge existing measures with new measures.
     * A measure is merged of the timestamps differ less than 2 minutes (since netatmo takes a measure every 5 minutes)
     * During a merge, the the value of the most recent measurement is taken, if available.
     *
     * @param measures
     * @param newMeasures
     * @return
     */
    public List<Measures> mergeMeasures(List<Measures> measures, List<Measures> newMeasuresList, long timestepTolerance) {

        List<Measures> result = new ArrayList<>();

        for (Measures n : newMeasuresList) 
        {
            boolean mergedMeasure = false;
            for (Measures m : measures) 
            {
                if (Math.abs(m.getTimestamp() - n.getTimestamp()) < timestepTolerance) 
                {
                    n.merge(m);
                    mergedMeasure = true;
                }
            }
            if (mergedMeasure) 
            {
                result.add(n);
            }
        }
        return result;


    }

    public List<Measures> getMeasures(String token, String device, String module, String measureTypes, String scale, long dateBegin, String dateEnd) {
        HashMap<String, String> params = new HashMap<>();
        params.put(ACCESS_TOKEN, token);
        params.put("device_id", device);
        if (module != null) 
        {
            params.put("module_id", module);
        }
        params.put("type", measureTypes);
        params.put("scale", scale);
        
        if ("last".equals(dateEnd))
        {
        	params.put("date_end", "" + dateEnd);
        }
        
        params.put("date_begin", "" + dateBegin);        	
        params.put("optimize", "false"); // easy parsing.

        List<Measures> measuresList = new ArrayList<>();
        try 
        {
            JSONParser parser = new JSONParser();
            String result = netatmoHttpClient.post(new URL(URL_GET_MEASURES_LIST), params);
            Object obj = parser.parse(result);
            JSONObject jsonResult = (JSONObject) obj;
            if (!(jsonResult.get("body") instanceof JSONObject)) 
            {
                logger.info("No data found");
                return measuresList;
            }
            JSONObject body = (JSONObject) jsonResult.get("body");

            for (Object o: body.keySet()) 
            {
                String timeStamp = (String) o;
                JSONArray valuesArray = (JSONArray) body.get(timeStamp);
                Measures measures = new Measures();
                long times = Long.parseLong(timeStamp) * 1000;
                measures.setTimestamp(times);
                
                if (measureTypes.equals("Pressure") && valuesArray.get(0) != null) 
                {
                    measures.setPressure(Double.parseDouble("" + valuesArray.get(0)));
                } 
                else if (measureTypes.equals("Rain"))
                {
                	measures.setRain(Double.parseDouble("" + valuesArray.get(0)));
                }
                else if (measureTypes.equals("sum_rain"))
        		{
            		measures.setRainAccumulated(Double.parseDouble("" + valuesArray.get(0)));	
	    		}
                else if (measureTypes.equals("Temperature,Humidity"))
                {
                	measures.setTemperature(Double.parseDouble("" + valuesArray.get(0)));
                	measures.setHumidity(Double.parseDouble("" + valuesArray.get(1)));
                }
                else if (measureTypes.equals("WindStrength,WindAngle,GustStrength,GustAngle"))
                {
                	measures.setWind(Double.parseDouble("" + valuesArray.get(0)),
                			Double.parseDouble("" + valuesArray.get(1)),
        					Double.parseDouble("" + valuesArray.get(2)),
							Double.parseDouble("" + valuesArray.get(3)));
                }
                
                measuresList.add(measures);
            }

            return measuresList;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Device getDevicesAndRefreskTokenIfNeeded(String token) {
        Device device = new Device();
        HashMap<String, String> params = new HashMap<>();
        params.put(ACCESS_TOKEN,token);
        
        try 
        {
            JSONParser parser = new JSONParser();
            String result = netatmoHttpClient.post(new URL(URL_GET_STATION_DATA), params);
            Object obj = parser.parse(result);
            JSONObject jsonResult = (JSONObject) obj;
            JSONObject body = (JSONObject) jsonResult.get("body");
            if (body == null)
            {
            	token = refreshTokens();
            	params.put(ACCESS_TOKEN,token);
            	result = netatmoHttpClient.post(new URL(URL_GET_STATION_DATA), params);
                obj = parser.parse(result);
                jsonResult = (JSONObject) obj;
                body = (JSONObject) jsonResult.get("body");            	
            }
            JSONArray devices = (JSONArray) body.get("devices");
            if (!devices.isEmpty())
            {            	
            	JSONObject firstDevice = (JSONObject) devices.get(0);            
            	String deviceId = (String) firstDevice.get("_id");            	
            	JSONArray modules = (JSONArray) firstDevice.get("modules");
            	
            	for (int i = 0; i < modules.size(); i++) 
            	{
            		JSONObject module = (JSONObject) modules.get(i);
            		String moduleId = (String) module.get("_id");
            		JSONArray dataTypes = (JSONArray) module.get("data_type");
            		if (!dataTypes.isEmpty()) 
            		{
            			@SuppressWarnings("unchecked")
						String joinedDataTypes = String.join(",", dataTypes);  
            			if (joinedDataTypes.equals("Wind"))
            			{
            				joinedDataTypes = "WindStrength,WindAngle,GustStrength,GustAngle";
            			}
                        device.addModuleToDevice(deviceId, moduleId, joinedDataTypes);
            		}
            	}
            }            

            return device;
        } 
        catch (Exception e) 
        {
            throw new RuntimeException(e);
        }
    }

    private String refreshTokens()
    {
    	String newAccessToken = null;
    	String refreshToken = netatmoTokenFiles.readToken(NetatmoTokenType.REFRESH);
    	HashMap<String, String> params = new HashMap<>();
        params.put("grant_type", REFRESH_TOKEN);
        params.put(REFRESH_TOKEN, refreshToken);
        params.put("client_id", clientId);
        params.put("client_secret", clientSecret);
        try {
            JSONParser parser = new JSONParser();
            String result = netatmoHttpClient.post(new URL(URL_REQUEST_TOKEN), params);
            Object obj = parser.parse(result);
            JSONObject jsonResult = (JSONObject) obj;
            newAccessToken = (String) jsonResult.get(ACCESS_TOKEN);
            String newRefreshToken = (String) jsonResult.get(REFRESH_TOKEN);
            Long expiresIn = (Long) jsonResult.get("expires_in");
            if(newAccessToken != null && !newAccessToken.isEmpty())
            	netatmoTokenFiles.writeToken(NetatmoTokenType.ACCESS, newAccessToken);
            if(newRefreshToken != null && !newRefreshToken.isEmpty())
            	netatmoTokenFiles.writeToken(NetatmoTokenType.REFRESH, newRefreshToken);
            logger.info("Refreshed access_token. New access_token expires in {} seconds", expiresIn.intValue());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return newAccessToken;
    }
}
