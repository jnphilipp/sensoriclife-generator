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
import org.sensoriclife.generator.world.WorldGenerator;
import org.sensoriclife.util.Helpers;

/**
 * 
 * @author paul, jnphilipp
 * @version 0.2.0
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
		if ( !WorldGenerator.isCreated() ) {
			Utils.sleep(5000);
			return;
		}

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
				entries = Accumulo.getInstance().scanAll(this.confs.get("generator.table_name"));
			}
			catch ( TableNotFoundException e ) {
				Logger.error(WaterGenerator.class, "Error while reading data from: " + this.confs.get("generator.table_name"), e.toString());
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
				
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
				sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
				String formattedTime = sdf.format(new Date(timestamp.getTime()));
				
				this.collector.emit(new Values(unit.getHotWaterID(), unit.getColdWaterID(), formattedTime , unit.getHotWaterMeter(), unit.getColdWaterMeter()));
				this.collector.emit("", null);
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
