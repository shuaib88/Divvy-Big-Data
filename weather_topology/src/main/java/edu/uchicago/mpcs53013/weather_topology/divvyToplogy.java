package edu.uchicago.mpcs53013.weather_topology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class divvyToplogy {

	static class FilterAirportsBolt extends BaseBasicBolt {
		//Pattern stationPattern;
		//Pattern weatherPattern;
		Pattern datePattern;
		Pattern triptimePattern;
		Pattern maxtempPattern;
		Pattern mintempPattern;
		Pattern prcpPattern;
		Pattern snowPattern;
		
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			//stationPattern = Pattern.compile("<station_id>K([A-Z]{3})</station_id>");
			//weatherPattern = Pattern.compile("<weather>([^<]*)</weather>");
			datePattern = Pattern.compile("<date>(.*)</date>");
			triptimePattern = Pattern.compile("<trip_time>(.*)</trip_time>");
			maxtempPattern = Pattern.compile("<max_temp>(.*)</max_temp>");
			mintempPattern = Pattern.compile("<min_temp>(.*)</min_temp>");
			prcpPattern = Pattern.compile("<precipitation>(.*)</precipitation>");
			snowPattern = Pattern.compile("<snow>(.*)</snow>");
			
			super.prepare(stormConf, context);
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String report = tuple.getString(0);
			/*Matcher stationMatcher = stationPattern.matcher(report);
			if(!stationMatcher.find()) {
				return;
			}
			Matcher weatherMatcher = weatherPattern.matcher(report);
			if(!weatherMatcher.find()) {
				return;
			}*/
			Matcher dateMatcher = datePattern.matcher(report);
			if(!dateMatcher.find()) {
				return;
			}
			Matcher trip_count_Matcher = triptimePattern.matcher(report);
			if(!trip_count_Matcher.find()) {
				return;
			}
			Matcher maxtempMatcher = maxtempPattern.matcher(report);
			if(!maxtempMatcher.find()) {
				return;
			}
			Matcher mintempMatcher = mintempPattern.matcher(report);
			if(!mintempMatcher.find()) {
				return;
			}
			Matcher prcpMatcher = prcpPattern.matcher(report);
			if(!prcpMatcher.find()) {
				return;
			}
			Matcher snowMatcher = snowPattern.matcher(report);
			if(!snowMatcher.find()) {
				return;
			}
			//remove extra values
			//collector.emit(new Values(stationMatcher.group(1), weatherMatcher.group(1)));
			collector.emit(new Values(dateMatcher.group(1), trip_count_Matcher.group(1), 
							maxtempMatcher.group(1), mintempMatcher.group(1), prcpMatcher.group(1),
							snowMatcher.group(1)));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("date", "trip_count", "max_temp", "min_temp", "prcp", "snow"));
		}

	}

	static class ExtractWeatherBolt extends BaseBasicBolt {

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			String date = input.getStringByField("date");
			String trip_count = input.getStringByField("trip_count");
			collector.emit(new Values
					(input.getStringByField("date"), input.getStringByField("trip_count"),
							input.getStringByField("max_temp"), input.getStringByField("min_temp"),
							input.getStringByField("prcp"), input.getStringByField("snow")
							));
		}

		@Override
		//NEED TO DECLARE MY NEW FIELDS
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("date", "trip_count", "max_temp", "min_temp", "prcp", "snow"));
		}

	}

	static class UpdateCurrentWeatherBolt extends BaseBasicBolt {
		private org.apache.hadoop.conf.Configuration conf;
		private HConnection hConnection;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			try {
				conf = HBaseConfiguration.create();
				conf.set("zookeeper.znode.parent", "/hbase-unsecure");
				hConnection = HConnectionManager.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.prepare(stormConf, context);
		}

		@Override
		public void cleanup() {
			try {
				hConnection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// TODO Auto-generated method stub
			super.cleanup();
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			try {
				HTableInterface table = hConnection.getTable("new_trip_data");
				Put put = new Put(Bytes.toBytes(input.getStringByField("date")));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("trip_count"), Bytes.toBytes(input.getStringByField("trip_count")));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("max_temp"), Bytes.toBytes(input.getStringByField("max_temp")));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("min_temp"), Bytes.toBytes(input.getStringByField("min_temp")));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("prcp"), Bytes.toBytes(input.getStringByField("prcp")));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("snow"), Bytes.toBytes(input.getStringByField("snow")));
				table.put(put);
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

		String zkIp = "localhost";

		String zookeeperHost = zkIp +":2181";

		ZkHosts zkHosts = new ZkHosts(zookeeperHost);
		List<String> zkServers = new ArrayList<String>();
		zkServers.add(zkIp);
		//make sure to swap test ID with some unique number, destination folder
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "ahmed_divvy", "/ahmed_divvy_events","362746");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		kafkaConfig.zkServers = zkServers;
		//the destination folder
		kafkaConfig.zkRoot = "/ahmed_divvy_events";
		kafkaConfig.zkPort = 2181;
		kafkaConfig.forceFromStart = true;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("new_divvy_spout", kafkaSpout, 1);
		builder.setBolt("filter-divvy", new FilterAirportsBolt(), 1).shuffleGrouping("new_divvy_spout");
		builder.setBolt("extract-divvy", new ExtractWeatherBolt(), 1).shuffleGrouping("filter-divvy");
		builder.setBolt("update-current-divvy", new UpdateCurrentWeatherBolt(), 1).fieldsGrouping("extract-divvy", new Fields("date"));


		Map conf = new HashMap();
		conf.put(backtype.storm.Config.TOPOLOGY_WORKERS, 4);
		conf.put(backtype.storm.Config.TOPOLOGY_DEBUG, true);
		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}   else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("divy_topology", conf, builder.createTopology());
		}
	}
}
