package org.sensoriclife.generator.electricity;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.json.simple.JSONObject;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.generator.world.ResidentialUnit;
import org.sensoriclife.generator.world.WorldGenerator;
import org.sensoriclife.util.Helpers;

/**
 * 
 * @author paul, stefan, jnphilipp
 * @version 0.1.0
 */
public class ElectricityGenerator extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private Date timestamp = new Timestamp(System.currentTimeMillis());
	private ElectricityValueGenerator valueGenerator = new ElectricityValueGenerator();

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		Logger.debug(ElectricityGenerator.class, "Generating next heating values.");
		if ( !WorldGenerator.isCreated() ) {
			Utils.sleep(5000);
			return;
		}

		Iterator<Map.Entry<Key, Value>> entries = null;
		try {
			entries = Accumulo.getInstance().scanAll(Config.getProperty("generator.table_name"));
		}
		catch ( TableNotFoundException e ) {
			Logger.error(ElectricityGenerator.class, "Error while reading data from: " + Config.getProperty("generator.table_name"), e.toString());
			return;
		}

		while ( entries.hasNext() ) {
			Map.Entry<Key, Value> entry = entries.next();
			ResidentialUnit unit;
			try {
				unit = (ResidentialUnit) Helpers.toObject(entry.getValue().get());
			} 
			catch ( IOException | ClassNotFoundException e ) {
				Logger.error(ElectricityGenerator.class, "Error while converting byte array to object.", e.toString());
				continue;
			}

			unit.setElectricityMeter(valueGenerator.generateNextValue(unit, timestamp));
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			
			JSONObject json = new JSONObject();
			json.put("id", unit.getElectricityID());
			json.put("time", sdf.format(new Date(timestamp.getTime())));
			json.put("value", unit.getElectricityMeter());
			this.collector.emit(new Values(json.toJSONString()));
			
			timestamp.setTime(timestamp.getTime()+15*60*1000);
		}

		if ( Config.getBooleanProperty("generator.realtime")) {
			Utils.sleep(900000 / Config.getIntegerProperty("generator.timefactor"));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("electricity"));
	}
}
