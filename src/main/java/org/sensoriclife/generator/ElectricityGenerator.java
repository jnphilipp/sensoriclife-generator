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
 * @author paul, stefan
 * @version 0.0.3
 */
public class ElectricityGenerator extends BaseRichSpout {

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
			Logger.error(ElectricityGenerator.class, e.toString());
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

		for (ResidentialUnit unit : residentialList) {
			int val = unit.getElectricityMeter() + (int) (unit.getPersons() * 0.2);
			unit.setElectricityMeter(val);

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			
			JSONObject json = new JSONObject();
			json.put("id", unit.getElectricityID());
			json.put("time", sdf.format(new Date(timestamp.getTime())));
			json.put("value", unit.getElectricityMeter());
			this.collector.emit(new Values(json.toJSONString()));
			
			timestamp.setTime(timestamp.getTime()+15*60*1000);
		}

		if (App.getBooleanProperty("realtime")) {
			try {
				Thread.sleep(1000);// for testing only 1 sec
				//Thread.sleep(900000);
			} catch (Exception e) {
				Logger.error(ElectricityGenerator.class, e.toString());
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("electricity"));
	}
}
