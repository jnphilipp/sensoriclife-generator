package org.sensoriclife.generator.water;

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
import java.util.Map.Entry;
import java.util.TimeZone;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.generator.world.ResidentialUnit;
import org.sensoriclife.util.Helpers;

/**
 * 
 * @author paul, jnphilipp
 * @version 0.2.2
 */
public class WaterGenerator extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private Date timestamp = new Timestamp(System.currentTimeMillis());
	private WaterValueGenerator valueGenerator = new WaterValueGenerator();
	private Map<String, String> confs;

	public WaterGenerator() {
		this.confs = Config.toMap();
	}

	public WaterGenerator(Map<String, String> confs) {
		this.confs = confs;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		Logger.debug(WaterGenerator.class, "Generating next water values.");

		try {
			if ( (((this.confs.containsKey("accumulo.name") && !this.confs.get("accumulo.name").isEmpty()) || (this.confs.containsKey("accumulo.zooServers") && !this.confs.get("accumulo.zooServers").isEmpty())) || (this.confs.containsKey("accumulo.user") && !this.confs.get("accumulo.user").isEmpty())) || (this.confs.containsKey("accumulo.password") && !this.confs.get("accumulo.password").isEmpty()) )
				Accumulo.getInstance().connect(this.confs.get("accumulo.name"), this.confs.get("accumulo.zooServers"), this.confs.get("accumulo.user"), this.confs.get("accumulo.password"));
		}
		catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error(WaterGenerator.class, "Error while connecting to accumulo.", e.toString());
		}

		for ( int i = 0; i < 4; i++ ) {//4x15min
			Iterator<Entry<Key, Value>> entries = null;
			try {
				entries = Accumulo.getInstance().scanAll(this.confs.get("generator.table_name_water"));
			}
			catch ( TableNotFoundException e ) {
				Logger.error(WaterGenerator.class, "Error while reading data from: " + this.confs.get("generator.table_name_water"), e.toString());
				break;
			}

			while ( entries.hasNext() ) {
				Entry<Key, Value> entry = entries.next();
				ResidentialUnit unit;

				try {
					unit = (ResidentialUnit)Helpers.toObject(entry.getValue().get());
				}
				catch ( IOException | ClassNotFoundException e ) {
					Logger.error(WaterGenerator.class, "Error while converting byte array to object.", e.toString());
					continue;
				}

				unit.setColdWaterMeter(valueGenerator.generateNextValue(unit, timestamp, WaterType.COLD));
				unit.setHotWaterMeter(valueGenerator.generateNextValue(unit, timestamp, WaterType.WARM));
				
				try {
					Value val  = new Value(Helpers.toByteArray(unit));
					String tableName = this.confs.get("generator.table_name_water");
					Accumulo.getInstance().addMutation(tableName, entry.getKey().toString(), "residentialUnit", "", val);
				} catch (Exception e) {
					Logger.error(WaterGenerator.class, "could not write new consumption to database");
				}
				

				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
				sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
				String formattedTime = sdf.format(new Date(timestamp.getTime()));

				this.collector.emit("hotwater", new Values(unit.getHotWaterID(), unit.getHotWaterMeter(), formattedTime));
				this.collector.emit("coldwater", new Values(unit.getColdWaterID(), unit.getColdWaterMeter(), formattedTime));
				timestamp.setTime(timestamp.getTime()+15*60*1000);
			}
		}

		if ( Boolean.parseBoolean(this.confs.get("generator.realtime")) )
			Utils.sleep((4*900000) / Integer.parseInt(this.confs.get("generator.timefactor")));//1h
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("hotwater", new Fields("hotwater_id", "hotWaterMeter", "time"));
		declarer.declareStream("coldwater", new Fields("coldwater_id", "coldWaterMeter", "time"));
	}
}
