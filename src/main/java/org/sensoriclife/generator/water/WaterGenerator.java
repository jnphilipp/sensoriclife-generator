package org.sensoriclife.generator.water;

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
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.generator.world.ResidentialUnit;
import org.sensoriclife.util.Helpers;

/**
 * 
 * @author paul
 * @version 0.0.4
 */
public class WaterGenerator extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private Date timestamp = new Timestamp(System.currentTimeMillis());
	private WaterValueGenerator valueGenerator = new WaterValueGenerator();

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		for(int i=0;i<4;i++)//4x15min
		{
			Iterator<Map.Entry<Key, Value>> entries = null;
			try {
				entries = Accumulo.getInstance().scanAll("generator_helper_table", "public");
			}
			catch (TableNotFoundException ex) {
				java.util.logging.Logger.getLogger(WaterGenerator.class.getName()).log(Level.SEVERE, null, ex);
			}
			while ( entries.hasNext() ) {
				Map.Entry<Key, Value> entry = entries.next();
				ResidentialUnit unit = null;
				try {
					unit = (ResidentialUnit) Helpers.toObject(entry.getValue().get());
				} 
				catch (IOException | ClassNotFoundException ex) {
					java.util.logging.Logger.getLogger(WaterGenerator.class.getName()).log(Level.SEVERE, null, ex);
				}
				
				unit.setColdWaterMeter(valueGenerator.generateNextValue(unit.getColdWaterID(), unit.getColdWaterMeter(), timestamp, unit.getPersons()));
				unit.setHotWaterMeter(valueGenerator.generateNextValue(unit.getHotWaterID(), unit.getHotWaterMeter(), timestamp, unit.getPersons()));
				
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
				sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
				String formattedTime = sdf.format(new Date(timestamp.getTime()));
				
				this.collector.emit(new Values(unit.getHotWaterID(), unit.getColdWaterID(), formattedTime , unit.getHotWaterMeter(), unit.getColdWaterMeter()));
				this.collector.emit("", null);
				timestamp.setTime(timestamp.getTime()+15*60*1000);
			}
		}

		if (Config.getBooleanProperty("realtime")) {
			try {
				Thread.sleep((4*1000)/Config.getIntegerProperty("timefactor"));// for testing only 4 sec
				//Thread.sleep((4*900000)/App.getIntegerProperty("timefactor"));//1h
			} catch (Exception e) {
				Logger.error(WaterGenerator.class, e.toString());
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("hotwater", new Fields("hotwater_id", "hotWaterMeter", "time"));
		declarer.declareStream("coldwater", new Fields("coldwater_id", "coldWaterMeter", "time"));
	}
}
