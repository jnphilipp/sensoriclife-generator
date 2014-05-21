package org.sensoriclife.generator.heating;

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
public class HeatingGenerator extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private Date timestamp = new Timestamp(System.currentTimeMillis());
	HeatingValueGenerator valueGenerator = new HeatingValueGenerator();

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		Logger.debug(HeatingGenerator.class, "Generating next heating values.");

		Iterator<Map.Entry<Key, Value>> entries = null;
		try {
			entries = Accumulo.getInstance().scanAll(Config.getProperty("generator.table_name"));
		}
		catch ( TableNotFoundException e ) {
			Logger.error(HeatingGenerator.class, "Error while reading data from: " + Config.getProperty("generator.table_name"), e.toString());
			return;
		}

		while ( entries.hasNext() ) {
			Map.Entry<Key, Value> entry = entries.next();
			ResidentialUnit unit;
			try {
				unit = (ResidentialUnit) Helpers.toObject(entry.getValue().get());
			} 
			catch ( IOException | ClassNotFoundException e ) {
				Logger.error(HeatingGenerator.class, "Error while converting byte array to object.", e.toString());
				continue;
			}
			
			unit.setHeatingMeters(valueGenerator.generateNextValue(unit, timestamp));			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			String formattedTime = sdf.format(new Date(timestamp.getTime()));

			String xml="<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><heating>";
			for ( int i = 0; i < unit.getHeatingIDs().length; i++ )
				xml += "<id>"+unit.getHeatingIDs()[i]+"</id><meter>"+unit.getHeatingMeters()[i]+"</meter>";

			xml += "<time>"+ formattedTime +"</time><heating>";
			this.collector.emit(new Values(xml));
			timestamp.setTime(timestamp.getTime()+15 * 60 * 1000);
		}

		if ( Config.getBooleanProperty("generator.realtime")) {
			try {
				Thread.sleep((1000)/Config.getIntegerProperty("generator.timefactor"));// for testing only 1 sec
				//Thread.sleep((900000)/App.getIntegerProperty("timefactor"));//15min
			}
			catch ( InterruptedException e ) {
				Logger.error(HeatingGenerator.class, e.toString());
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("heating"));
	}
}
