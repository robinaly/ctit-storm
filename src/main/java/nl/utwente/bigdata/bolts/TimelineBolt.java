/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.utwente.bigdata.bolts;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import twitter4j.Paging;
import twitter4j.RateLimitStatus;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TimelineBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 394263766896592119L;
	private transient Twitter twitter = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		String consumerKey = (String) stormConf.get("consumerKey");
		String consumerSecret = (String) stormConf.get("consumerSecret");
		String accessToken = (String) stormConf.get("accessToken");
		String accessTokenSecret = (String) stormConf.get("accessTokenSecret");
		System.out.printf("%s %s %s %s\n", consumerKey, consumerSecret, accessToken, accessTokenSecret);
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey(consumerKey)
		  .setOAuthConsumerSecret(consumerSecret)
		  .setOAuthAccessToken(accessToken)
		  .setOAuthAccessTokenSecret(accessTokenSecret)
		  .setJSONStoreEnabled(true);
		TwitterFactory tf = new TwitterFactory(cb.build());
		twitter = tf.getInstance();
		try {
			getRateLimit();
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private long timelineRemaining = 0;
	private long timelineNextTime = 0;
	private void getRateLimit() throws TwitterException {
		Map<String, RateLimitStatus> rates = twitter.getRateLimitStatus();
		for (Entry<String, RateLimitStatus> x: rates.entrySet()) {
			if (x.getKey().equals("/statuses/user_timeline")) {
				this.timelineRemaining = x.getValue().getRemaining();
				timelineNextTime = x.getValue().getResetTimeInSeconds() * 1000l;	
			}			
		}
		System.out.println("timelineRemaining: " + timelineRemaining);
		System.out.println("timelineNextTime: " + timelineNextTime);
	}
	
	private List<Status> getTimeline(long id) throws Exception {
		int COUNT = 10;
		int GET = 10;
		List<Status> stati = new LinkedList<Status>();
		int page = 1;
		long timelineMaxId = Integer.MAX_VALUE;
		while (stati.size() < GET) {
			while (timelineRemaining < 5) {
				getRateLimit();
				long sleep = Math.max(timelineNextTime - System.currentTimeMillis(), 10000);
				System.out.println("Sleeping " + sleep);
				Thread.sleep(Math.min(sleep, 60 * 1000));
			}
			Paging paging  = new Paging(page, COUNT, 1, timelineMaxId - 1);
			List<Status> statuses = twitter.getUserTimeline(id, paging);			
			
			for (Status status: statuses) {
				if (status != null) {			
					stati.add(status);					
					timelineMaxId = Math.min(status.getId(), timelineMaxId);
				}
			}
			
		}
		return stati;
	}
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if (twitter == null) return;
		
		try {
			System.out.println(tuple);
			String d = tuple.getString(0);
			System.out.println("Retrieving id: " + d);
			long id = Long.parseLong(d);
			
			for (Status s: getTimeline(id)) {
				String statusS = DataObjectFactory.getRawJSON(s);			
				collector.emit(new Values(s.getId()+"", statusS));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "tweet"));
	}

//	@Override
//	public Map<String, Object> getComponentConfiguration() {
//		//Map<String, Object> conf = new HashMap<String, Object>();
//		//conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
//		return conf;
//	}
}
