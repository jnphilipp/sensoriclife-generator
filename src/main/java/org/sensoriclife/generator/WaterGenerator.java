package org.sensoriclife.generator;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import org.json.simple.JSONObject;
import org.sensoriclife.Logger;

/**
 * 
 * @author paul
 * @version 0.0.1
 */
public class WaterGenerator extends BaseRichSpout {

	private ArrayList<ResidentialUnit> residentialList = new ArrayList<ResidentialUnit>();
	private SpoutOutputCollector collector;
	private Date timestamp = new Timestamp(System.currentTimeMillis());

	
	/*
	 * throws FileNotFoundException if WorldGenerator is still in processing and didn't create the .ser file yet
	 */
	private void deserializing() throws FileNotFoundException{
		try {
			ObjectInputStream o = new ObjectInputStream(new FileInputStream(WorldGenerator.PATH_OUTPUT_FILE));
			this.residentialList = (ArrayList<ResidentialUnit>) o.readObject();
			o.close();
		} catch (IOException | ClassNotFoundException e) {
			Logger.error(WaterGenerator.class, e.toString());
		}		
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		if (residentialList == null){
			try{
				deserializing();	
			}catch(FileNotFoundException f){
				return;
			}
		}			

		int timeflag=0;
		for (ResidentialUnit unit : residentialList) {
			
			int valHot = unit.getHotWaterMeter() + (int) (unit.getPersons() * 25);
			unit.setHotWaterMeter(valHot);
			int valCold = unit.getColdWaterMeter() + (int) (unit.getPersons() * 50);
			unit.setColdWaterMeter(valCold);

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			
			if(this.collector!=null)
									this.collector.emit(new Values( unit.getWaterID(), unit.getHotWaterMeter(), unit.getColdWaterMeter(), sdf.format(new Date(timestamp.getTime())) ));
			
			if(timeflag%4==0){
				timestamp.setTime(timestamp.getTime()+60*60*1000);
				timeflag=0;
			}
			timeflag++;
		}

		if (App.getBooleanProperty("realtime")) {
			try {
				Thread.sleep(4*1000);// for testing only 4 sec
				//Thread.sleep(4*900000);//1h
			} catch (Exception e) {
				Logger.error(WaterGenerator.class, e.toString());
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("water"));
	}
}
