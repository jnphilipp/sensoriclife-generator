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
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.json.simple.JSONObject;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.generator.world.ResidentialUnit;
import org.sensoriclife.util.Helpers;

/**
 * 
 * @author paul, stefan, jnphilipp
 * @version 0.2.1
 */
public class ElectricityGenerator extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private Date timestamp = new Timestamp(System.currentTimeMillis());
	private ElectricityValueGenerator valueGenerator = new ElectricityValueGenerator();
	private Map<String, String> confs;

	public ElectricityGenerator() {
		this.confs = Config.toMap();
	}

	public ElectricityGenerator(Map<String, String> confs) {
		this.confs = confs;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		Logger.debug(ElectricityGenerator.class, "Generating next heating values.");

		try {
			if ( (((this.confs.containsKey("accumulo.name") && !this.confs.get("accumulo.name").isEmpty()) || (this.confs.containsKey("accumulo.zooServers") && !this.confs.get("accumulo.zooServers").isEmpty())) || (this.confs.containsKey("accumulo.user") && !this.confs.get("accumulo.user").isEmpty())) || (this.confs.containsKey("accumulo.password") && !this.confs.get("accumulo.password").isEmpty()) )
				Accumulo.getInstance().connect(this.confs.get("accumulo.name"), this.confs.get("accumulo.zooServers"), this.confs.get("accumulo.user"), this.confs.get("accumulo.password"));
		}
		catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error(ElectricityGenerator.class, "Error while connecting to accumulo.", e.toString());
		}

		Iterator<Map.Entry<Key, Value>> entries = null;
		try {
			entries = Accumulo.getInstance().scanAll(this.confs.get("generator.table_name_electricity"));
		}
		catch ( TableNotFoundException e ) {
			Logger.error(ElectricityGenerator.class, "Error while reading data from: " + this.confs.get("generator.table_name_electricity"), e.toString());
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
			try {
				Value val  = new Value(Helpers.toByteArray(unit));
				String tableName = this.confs.get("generator.table_name_electricity");
				Accumulo.getInstance().addMutation(tableName, entry.getKey().toString(), "residentialUnit", "", val);
			} catch (Exception e) {
				Logger.error(ElectricityGenerator.class, "could not write new consumption to database");
			}
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			
			JSONObject json = new JSONObject();
			json.put("id", unit.getElectricityID());
			json.put("time", sdf.format(new Date(timestamp.getTime())));
			json.put("value", unit.getElectricityMeter());
			this.collector.emit(new Values(json.toJSONString()));
			
			timestamp.setTime(timestamp.getTime()+15*60*1000);
		}

		if ( Boolean.parseBoolean(this.confs.get("generator.realtime")) )
			Utils.sleep(900000 / Integer.parseInt(this.confs.get("generator.timefactor")));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("electricity"));
	}
}
