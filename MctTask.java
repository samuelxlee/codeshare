package com.delta.skdchange.mct;

import com.delta.api.authorization.client.DefaultApi;
import com.delta.api.authorization.model.TokenInfo;
import com.delta.api.iatamct.client.RetrieveMinimumConnectTimeApi;
import com.delta.api.iatamct.model.AirportMinimumConnectTime;
import com.delta.api.iatamct.model.AirportMinimumConnectTimeList;
import com.delta.api.iatamct.model.BatchFlightLegList;
import com.delta.api.iatamct.model.FlightLegList;
import com.delta.skdchange.og.Reference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.RecursiveTask;


public class MctTask extends RecursiveTask<HashMap<String, AirportMinimumConnectTime>> {
    static final Logger log = LogManager.getLogger(MctTask.class);
    static boolean logRequest = true;
    static Reference ref;

	private String clientId;
	private String clientSecret;
	private String oauthUrl;
	private String mctUrl;
    private String transactionId;
    private HashMap<String, FltLegPair> fltLegMap;

	public static void set(Reference reference) {
		ref = reference;
	}
	
    public MctTask(String transactionId, HashMap<String, FltLegPair> fltMap) {
        this.transactionId = transactionId;
        this.fltLegMap = fltMap;
    }

    public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public void setClientSecret(String clientSecret) {
		this.clientSecret = clientSecret;
	}

	public void setOauthUrl(String oauthUrl) {
		this.oauthUrl = oauthUrl;
	}

	public void setMctUrl(String mctUrl) {
		this.mctUrl = mctUrl;
	}

	@Override
    protected HashMap<String, AirportMinimumConnectTime> compute() {
        if (fltLegMap.size() < ref.getMctBatchSize()) {
            HashMap<String, AirportMinimumConnectTime> mcts = getMcts(transactionId, fltLegMap);
            return mcts;
        } else {
            int count = 0;
            int middle = fltLegMap.size()/2;
            Iterator<FltLegPair> fltPairIt = fltLegMap.values().iterator();
            HashMap<String, FltLegPair> fltLegMap1 = new HashMap<String, FltLegPair>();
            HashMap<String, FltLegPair> fltLegMap2 = new HashMap<String, FltLegPair>();
            while (fltPairIt.hasNext()) {
                FltLegPair fltLegPair = fltPairIt.next();
                if (count++ < middle) {
                    fltLegMap1.put(fltLegPair.getKey(), fltLegPair);
                } else {
                    fltLegMap2.put(fltLegPair.getKey(), fltLegPair);
                }
            }
            MctTask task1 = new MctTask(transactionId, fltLegMap1);
            task1.setClientId(clientId);
            task1.setClientSecret(clientSecret);
            task1.setOauthUrl(oauthUrl);
            task1.setMctUrl(mctUrl);
            task1.fork();
            MctTask task2 = new MctTask(transactionId, fltLegMap2);
            task2.setClientId(clientId);
            task2.setClientSecret(clientSecret);
            task2.setOauthUrl(oauthUrl);
            task2.setMctUrl(mctUrl);
            HashMap<String, AirportMinimumConnectTime> mcts2 = task2.compute();
            HashMap<String, AirportMinimumConnectTime> mcts1 = task1.join();
//            int resp1Size = mcts1.size();
//            int resp2Size = mcts2.size();
            mcts1.putAll(mcts2);
//            log.debug("Mct responese count: " + mcts1.size());
            return mcts1;
        }
    }

    // the key for request HashMap is the the flight leg key pair: DL012320190811JFKDL023420190811ATL
    // the key for the response hashMap is the flight leg key pair: DL012320190811JFKDL023420190811ATL
    private HashMap<String, AirportMinimumConnectTime> getMcts(String transactionId, HashMap<String, FltLegPair> fltLegMap) {
        HashMap<String, FltLegPair> optionNumFltMap = new HashMap<String, FltLegPair>();
        Iterator<FltLegPair> fltLegPairs = fltLegMap.values().iterator();
        boolean batchMode = (fltLegMap.size()>10)?true:false;
        FlightLegList fltLegList = new FlightLegList();
        BatchFlightLegList batchFltLegList = new BatchFlightLegList();
        int count = 1;
        while (((Iterator) fltLegPairs).hasNext()) {
            FltLegPair fltLegPair = fltLegPairs.next();
            String optionNum = String.valueOf(count++);
            fltLegPair.setOptionNum(optionNum);
            optionNumFltMap.put(optionNum, fltLegPair);
            if (batchMode) {
                batchFltLegList.addBatchFlightLegsItem(fltLegPair.getLeg1());
                batchFltLegList.addBatchFlightLegsItem(fltLegPair.getLeg2());
            } else {
                fltLegList.addFlightLegsItem(fltLegPair.getLeg1());
                fltLegList.addFlightLegsItem(fltLegPair.getLeg2());
            }
        }

        AirportMinimumConnectTimeList mcts;
        if (batchMode) {
            mcts = callMctUrlBatch(transactionId, batchFltLegList);
            if (mcts != null && mcts.getAirportMinimumConnectTimes().size()*2 != batchFltLegList.getBatchFlightLegs().size()) {
                log.debug(transactionId + "request mcts: " + batchFltLegList.getBatchFlightLegs().size()/2 + ", response mcts: " + mcts.getAirportMinimumConnectTimes().size());
                try {
                    if (logRequest) {
                        ObjectMapper mapper = new ObjectMapper();
                        log.debug(transactionId + "mct request: " + mapper.writeValueAsString(batchFltLegList));
                        log.debug(transactionId + "mct response: " + mapper.writeValueAsString(mcts));
                        logRequest = false;
                    }
                } catch (Exception e) {
                }
            }
        } else {
            mcts = callMctUrl(transactionId, fltLegList);
            if (mcts != null && mcts.getAirportMinimumConnectTimes().size()*2 != fltLegList.getFlightLegs().size()) {
                log.debug(transactionId + "request mcts: " + fltLegList.getFlightLegs().size()/2 + ", response mcts: " + mcts.getAirportMinimumConnectTimes().size());
                try {
                    if (logRequest) {
                        ObjectMapper mapper = new ObjectMapper();
                        log.debug(transactionId + "mct request: " + mapper.writeValueAsString(fltLegList));
                        log.debug(transactionId + "mct response: " + mapper.writeValueAsString(mcts));
                        logRequest = false;
                    }
                } catch (Exception e) {
                }
            }
        }

        List<AirportMinimumConnectTime> mctList = new ArrayList<AirportMinimumConnectTime>();
        if (mcts != null)
            mctList = mcts.getAirportMinimumConnectTimes();
        HashMap<String, AirportMinimumConnectTime> mctMap = new HashMap<String, AirportMinimumConnectTime>();
        for (int i = 0; i < mctList.size(); i++) {
            AirportMinimumConnectTime mct = mctList.get(i);
            String optionNum = mct.getOptionNum();
            String optNum = String.valueOf(Integer.parseInt(optionNum));
            FltLegPair fltLegPair = optionNumFltMap.get(optNum);
            if (fltLegPair == null)
                continue;
            String key = fltLegPair.getKey();
            mctMap.put(key, mct);
        }
        return mctMap;
    }

    private AirportMinimumConnectTimeList callMctUrl(String transactionId, FlightLegList flightLegList) {
        try {
            AirportMinimumConnectTimeList mctResponse = callMctUrl1(transactionId, flightLegList);
            return mctResponse;
        }
        catch(Exception ex) {
            log.error(transactionId + "Failed to callMctUrl(): " + ex.getMessage() + " from url " + mctUrl + ", retrying ....");
            try {
                AirportMinimumConnectTimeList mctResponse = callMctUrl1(transactionId, flightLegList);
                return mctResponse;
            } catch (Exception e) {
                try {
                    if (logRequest) {
                        logRequest = false;
                        ObjectMapper mapper = new ObjectMapper();
                        log.debug(transactionId + "Failed mct request: " + mapper.writeValueAsString(flightLegList) + " from url: " + mctUrl);
                    }
                } catch (Exception xe) {}
            }
        }
        return null;
    }

    private AirportMinimumConnectTimeList callMctUrl1(String transactionId, FlightLegList flightLegList) throws Exception {
        DefaultApi authApi = new DefaultApi();
        String contentType = "application/json";
        String grantType = "client_credentials";
        String accept = "application/json";
        authApi.getApiClient().setBasePath(oauthUrl);
        TokenInfo authResponse = authApi.token(contentType, clientId, clientSecret, grantType, accept);

        RetrieveMinimumConnectTimeApi retrieveMinimumConnectTimeApi = new RetrieveMinimumConnectTimeApi();
        String channelId = "SRS";
        String appId = "VP";
        Boolean xMCTHierarchy = Boolean.FALSE;
        retrieveMinimumConnectTimeApi.getApiClient().setBasePath(mctUrl);
        retrieveMinimumConnectTimeApi.getApiClient().setAccessToken(authResponse.getAccessToken());
        AirportMinimumConnectTimeList mctResponse = retrieveMinimumConnectTimeApi.retrieveMinimumConnectTime(transactionId, channelId, appId, xMCTHierarchy, flightLegList);
        return mctResponse;
    }

    private AirportMinimumConnectTimeList callMctUrlBatch(String transactionId, BatchFlightLegList flightLegList) {
        try {
            AirportMinimumConnectTimeList mctResponse = callMctUrlBatch1(transactionId, flightLegList);
            return mctResponse;
        }
        catch(Exception ex) {
            log.error(transactionId + "Failed to callMctUrlBatch(): " + ex.getMessage() + " from url: " + mctUrl + ", retrying ....");
            try {
                AirportMinimumConnectTimeList mctResponse = callMctUrlBatch1(transactionId, flightLegList);
                return mctResponse;
            } catch (Exception e) {
                try {
                    if (logRequest) {
                        logRequest = false;
                        ObjectMapper mapper = new ObjectMapper();
                        log.debug(transactionId + "Failed batch mct request: " + mapper.writeValueAsString(flightLegList));
                    }
                } catch (Exception xe) {}
            }
        }
        return null;
    }

    private AirportMinimumConnectTimeList callMctUrlBatch1(String transactionId, BatchFlightLegList flightLegList) throws Exception {
        DefaultApi authApi = new DefaultApi();
        String contentType = "application/json";
        String grantType = "client_credentials";
        String accept = "application/json";
        authApi.getApiClient().setBasePath(oauthUrl);
        TokenInfo authResponse = authApi.token(contentType, clientId, clientSecret, grantType, accept);

        RetrieveMinimumConnectTimeApi retrieveMinimumConnectTimeApi = new RetrieveMinimumConnectTimeApi();
        String channelId = "SRS";
        String appId = "VP";
        Boolean xMCTHierarchy = Boolean.FALSE;
        retrieveMinimumConnectTimeApi.getApiClient().setBasePath(mctUrl);
        retrieveMinimumConnectTimeApi.getApiClient().setAccessToken(authResponse.getAccessToken());
        AirportMinimumConnectTimeList mctResponse = retrieveMinimumConnectTimeApi.retrieveBatchMinimumConnectTime(transactionId, channelId, appId, xMCTHierarchy, flightLegList);
        return mctResponse;
    }

}