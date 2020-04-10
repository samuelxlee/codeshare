package com.delta.skdchange.mct;


import com.delta.api.iatamct.model.FlightLeg;

import java.text.SimpleDateFormat;
import java.util.HashMap;

public class FltLegPair {
    private FlightLeg leg1;
    private FlightLeg leg2;
    private String key;

    public static void addFltLegPair(com.delta.skdchange.og.FlightLeg fltLeg1, com.delta.skdchange.og.FlightLeg fltLeg2, HashMap<String, FltLegPair> fltPairMap) {
        FltLegPair fltPair = new FltLegPair(fltLeg1, fltLeg2);
        fltPairMap.put(fltPair.getKey(), fltPair);
    }

    public FltLegPair(com.delta.skdchange.og.FlightLeg fltLeg1, com.delta.skdchange.og.FlightLeg fltLeg2) {
        leg1 = translate(fltLeg1, true);
        leg2 = translate(fltLeg2, false);
        key = fltLeg1.getMctKey() + fltLeg2.getMctKey();
    }

    public void setOptionNum(String optionNum) {
        leg1.setOptionNum(optionNum);
        leg2.setOptionNum(optionNum);
    }

    public FlightLeg getLeg1() {
        return leg1;
    }
    public FlightLeg getLeg2() {
        return leg2;
    }

    public String getKey() {
        return key;
    }
    private FlightLeg translate(com.delta.skdchange.og.FlightLeg fltLeg, boolean arrival) {
        FlightLeg leg = new FlightLeg();
        leg.setOriginAirportCode(fltLeg.getOrig());
        leg.setDestinationAirportCode(fltLeg.getDests()[0]);
        leg.setMarketingCarrierCode(fltLeg.getMkAirline());
        leg.setMarketingFlightNum(String.valueOf(fltLeg.getMkFltNum()));
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        leg.setFlightDepartureDate(dateFormat.format(fltLeg.getDepartureLT()));
        leg.setUsInternationalDomesticCode(fltLeg.isDomestic()?"D":"I");
        leg.setOperatingCarrierCode(fltLeg.getOpAirline());
        leg.setCodeshareFlight("L".equals(fltLeg.getFltType())?Boolean.TRUE : Boolean.FALSE);
        if (arrival)
           leg.setAirportTerminalId(fltLeg.getArrTerminal());
        else
            leg.setAirportTerminalId(fltLeg.getDepTerminal());
        if (leg.getAirportTerminalId() == null)
            leg.setAirportTerminalId("N/A");
        if (fltLeg.getIndsStdAcTypCd() == null)
            leg.setIndustryStandardAircraftTypeCode("N/A");
        else
            leg.setIndustryStandardAircraftTypeCode(fltLeg.getIndsStdAcTypCd());
        return leg;
    }

}