package com.ekkelenkamp.netatmo2wow;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Map;


public class NetatmoHttpClientImpl implements NetatmoHttpClient {

	static final Logger logger = LogManager.getLogger(NetatmoHttpClientImpl.class);

    static final String USER_AGENT = "Java Netatmo Importer";

    @SuppressWarnings("unused")
	private void NetatmoHttpClient() {
    	// Empty constructor
    }

    static String convertStreamToString(java.io.InputStream is) {
        try(java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A"))
        {
        	return s.hasNext() ? s.next() : "";
        }
    }

    @Override
    public String post(URL url, final Map<String, String> params) throws IOException, NoSuchAlgorithmException, KeyManagementException {
        // Create a trust manager that does not validate certificate chains
        // The netatmo ssl keys are not working without it.
        final TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
            @Override
            public void checkClientTrusted(final X509Certificate[] chain, final String authType) {
            	// Unused method
            }

            @Override
            public void checkServerTrusted(final X509Certificate[] chain, final String authType) {
            	// Unused method
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        }};

        // Install the all-trusting trust manager
        final SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        // Create an ssl socket factory with our all-trusting manager
        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

        URL urlObject = url;
        final HttpURLConnection connection = (HttpURLConnection) urlObject.openConnection();
        if (connection instanceof HttpsURLConnection) {
            HttpsURLConnection connectionHttps = (HttpsURLConnection) connection;
            connectionHttps.setSSLSocketFactory(sslSocketFactory);

        }
        connection.setDefaultUseCaches(false);
        connection.setUseCaches(false);
        connection.setRequestProperty("User-Agent", USER_AGENT);
        connection.setReadTimeout(10000);
        connection.setConnectTimeout(15000);
        connection.setRequestMethod("POST");
        connection.setDoInput(true);
        connection.setDoOutput(true);
        applyParams(connection, params);
        try {
            final int http_code = connection.getResponseCode();
            if (http_code == 200) { /* good code */
                String response = readStream(connection.getInputStream());
                connection.disconnect();
                return response;
            } else { /* error code*/
                String response = readStream(connection.getErrorStream());
                connection.disconnect();
                return response;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private String readStream(InputStream in) {
        String rv = null;
        StringBuilder sb = new StringBuilder();
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line = "";
            while ((line = reader.readLine()) != null) sb.append(line);
            rv = sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rv;
    }

    private boolean applyParams(HttpURLConnection connection, Map<String, String> paramsHash) {
        try {
            String params = createParamsLine(paramsHash);
            logger.debug("url: {}?{}", connection.getURL(), params);
            OutputStream os = connection.getOutputStream();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
            writer.write(params);
            writer.close();
            os.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private String createParamsLine(Map<String, String> p) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        if (p.size() > 0) for (Map.Entry<String, String> pair : p.entrySet()) {
            if (first) {
                first = false;
            } else {
                result.append("&");
            }
            if (pair.getValue() != null) {
               result.append(URLEncoder.encode(pair.getKey(), StandardCharsets.UTF_8)).append("=").append(URLEncoder.encode(pair.getValue(), StandardCharsets.UTF_8));
            }        
        }

        return result.toString();
    }

}
