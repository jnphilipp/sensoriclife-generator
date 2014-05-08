package org.sensoriclife.generator.electricity;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.json.simple.JSONObject;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.generator.App;
import org.sensoriclife.generator.world.ResidentialUnit;
import org.sensoriclife.util.Helpers;

/**
 * 
 * @author paul, stefan
 * @version 0.0.4
 */
public class ElectricityGenerator extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private Date timestamp = new Timestamp(System.currentTimeMillis());

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		Iterator<Map.Entry<Key, Value>> entries = null;
		try {
			entries = Accumulo.getInstance().scannAll("generator_helper_table", "public");
		} 
		catch (TableNotFoundException ex) {
			java.util.logging.Logger.getLogger(ElectricityGenerator.class.getName()).log(Level.SEVERE, null, ex);
		}
		while ( entries.hasNext() ) {
			Map.Entry<Key, Value> entry = entries.next();
			ResidentialUnit unit = null;
			try {
				unit = (ResidentialUnit) Helpers.toObject(entry.getValue().get());
			} 
			catch (IOException | ClassNotFoundException ex) {
				java.util.logging.Logger.getLogger(ElectricityGenerator.class.getName()).log(Level.SEVERE, null, ex);
			}

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
				Thread.sleep(1000/App.getIntegerProperty("timefactor"));// for testing only 1 sec
				//Thread.sleep(900000/App.getIntegerProperty("timefactor"));
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
