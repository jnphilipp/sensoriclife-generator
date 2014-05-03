package org.sensoriclife.generator;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Map;
import org.sensoriclife.Logger;
import org.sensoriclife.util.Helpers;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 
 * @author paul
 * @version 0.0.1
 */
public class ElectricityGenerator extends BaseRichSpout {

	private ArrayList<User> userList = new ArrayList<User>();
	private ArrayList<ResidentialUnit> residentialList = new ArrayList<ResidentialUnit>();
	private SpoutOutputCollector collector;
	private int timestamp = 0;

	private void deserializing() {
		try {
			ObjectInputStream o1 = new ObjectInputStream(new FileInputStream(Helpers.getUserDir() + "/data/userList.ser"));
			this.userList = (ArrayList<User>) o1.readObject();
			o1.close();

			ObjectInputStream o2 = new ObjectInputStream(new FileInputStream(Helpers.getUserDir() + "/data/residentialList.ser"));
			this.residentialList = (ArrayList<ResidentialUnit>) o2.readObject();
			o2.close();
		} catch (Exception e) {
			Logger.error(ElectricityGenerator.class, e.toString());
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		if (residentialList == null)
			deserializing();

		for (ResidentialUnit unit : residentialList) {
			int val = unit.getElectricityMeter() + (int) (unit.getPersons() * 0.2);
			unit.setElectricityMeter(val);
			timestamp += 15;

			this.collector.emit(new Values(unit.getElectricityID(), unit.getElectricityMeter(), timestamp));
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
		declarer.declare(new Fields("nextBolt")); // TODO: findname for nextBolt
	}
}
