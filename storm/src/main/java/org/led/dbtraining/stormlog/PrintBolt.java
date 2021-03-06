package org.led.dbtraining.stormlog;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class PrintBolt extends BaseBasicBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8329158424617426118L;
	private int indexId;
	
	public void prepare(Map stormConf, TopologyContext context) {
		this.indexId = context.getThisTaskId();
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String rec = input.getString(0);
		System.out.println(String.format("Bolt[%d] String received: %s", this.indexId, rec));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
}
