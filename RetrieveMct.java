package com.delta.skdchange.mct;

import com.delta.api.iatamct.model.AirportMinimumConnectTime;
import com.delta.skdchange.og.Reference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ForkJoinPool;

@Service
public class RetrieveMct {
    static final Logger log = LogManager.getLogger(RetrieveMct.class);
    static Reference ref;
    
	@Value("${srs.api.client.id}")
	private String clientId;
	@Value("${srs.api.client.secret}")
	private String clientSecret;
	@Value("${srs.api.client.oauthUrl}")
	private String oauthUrl;
	@Value("${srs.api.client.iatamctUrl}")
	private String mctUrl;
	
	public static void set(Reference reference) {
		ref = reference;
	}

    public HashMap<String, AirportMinimumConnectTime> getMcts(String transactionId, HashMap<String, FltLegPair> fltLegMap) {
    	if (fltLegMap.size() == 0) {
    		log.info(transactionId + " 0 mct connections requested.");
    		return new HashMap<String, AirportMinimumConnectTime>();
    	}
        long start = System.currentTimeMillis();
        log.debug(transactionId + " Mct connections - requesting: " + fltLegMap.size());
        HashMap<String, FltLegPair> map = new HashMap<String, FltLegPair>();
        Iterator<String> its = fltLegMap.keySet().iterator();
        while (its.hasNext()) {
            String key = its.next();
            map.put(key, fltLegMap.get(key));
        }
        MctTask.logRequest = true;
        fltLegMap = map;
        int threadConcurrency = (int)Math.round(fltLegMap.size()/ref.getMctBatchSize() + 0.5);
        threadConcurrency = (threadConcurrency>ref.getMaxMctThreads())?ref.getMaxMctThreads():threadConcurrency;    //set the max number of threads at 50
        ForkJoinPool pool = new ForkJoinPool(threadConcurrency);
        MctTask mctTask = new MctTask(transactionId, fltLegMap);
        mctTask.setClientId(clientId.trim());
        mctTask.setClientSecret(clientSecret.trim());
        mctTask.setOauthUrl(oauthUrl.trim());
        mctTask.setMctUrl(mctUrl.trim());
        pool.invoke(mctTask);
        HashMap<String, AirportMinimumConnectTime> mctMap = mctTask.join();
        pool.shutdown();
        log.info(transactionId + " Mct connections - requested: " + fltLegMap.size() + " items, actual response: " + mctMap.size() + " items, batch size: " + ref.getMctBatchSize() + ", parallel threads: " + threadConcurrency + ", time consumed: " + (System.currentTimeMillis() - start) + " ms.");
        return mctMap;
    }

}
