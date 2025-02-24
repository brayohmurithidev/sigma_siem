package com.fazitech;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DataStreamJob {
	// Kafka Configurations
	private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String SIGMA_RULES_TOPIC = "sigma_rules";
	private static final String EVENTS_TOPIC = "events";
	private static final String SUCCESS_TOPIC = "success_topic";
	private static final String FAILURE_TOPIC = "failure_topic";
	//private static final String EVENT_FILE_PATH = "/Users/fazitech/work/Java/sigma/events.json"; // Change the path to your event file

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ObjectMapper objectMapper = new ObjectMapper();



		KafkaSource<String> eventsSource = KafkaSource.<String>
				builder()
				.setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
				.setTopics(EVENTS_TOPIC)
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		// Read Sigma Rules from Kafka
		KafkaSource<String> sigmaRulesSource = KafkaSource.<String>builder()
				.setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setTopics(SIGMA_RULES_TOPIC)
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> eventStream = env.fromSource(
				eventsSource,
				WatermarkStrategy.noWatermarks(),
				"Event Source"
		);


		DataStream<String> rulesStream = env.fromSource(sigmaRulesSource, WatermarkStrategy.noWatermarks(), "Kafka Rules Source");

//		rulesStream.print("rules stream");

		// Kafka Sink for Success Topic
		KafkaSink<String> successSink = KafkaSink.<String>builder()
				.setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(SUCCESS_TOPIC)
						.setValueSerializationSchema(new SimpleStringSchema())
						.build())
				.build();

		// Kafka Sink for Failure Topic
		KafkaSink<String> failureSink = KafkaSink.<String>builder()
				.setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(FAILURE_TOPIC)
						.setValueSerializationSchema(new SimpleStringSchema())
						.build())
				.build();


//		Working with broadcast state:
		MapStateDescriptor<Void, List<JsonNode>> sigmaRuleDescriptor = new MapStateDescriptor<>("sigmaRules", Types.VOID, Types.LIST(Types.GENERIC(JsonNode.class)));

//		Broadcast the rules stream
		BroadcastStream<String> broadcastSigmaRules = rulesStream.broadcast(sigmaRuleDescriptor);

		//process events with broadcast state
		DataStream<String> processedEvents = eventStream.connect(broadcastSigmaRules)
				.process(new BroadcastProcessFunction<String, String, String>() {
					private transient List<JsonNode> sigmaRules;



					@Override
					public void processElement(String eventJson, ReadOnlyContext ctx, Collector<String> out) throws Exception {
						JsonNode event = objectMapper.readTree(eventJson);

						//get the latest rules from state
						ReadOnlyBroadcastState<Void, List<JsonNode>> broadcastState = ctx.getBroadcastState(sigmaRuleDescriptor);
						sigmaRules = broadcastState.get(null);



						if(sigmaRules != null) {
							for (JsonNode rule : sigmaRules) {
								if(matchesSigmaRule(event, rule)) {
									//add the rule title to list of matched rules
									if(!event.has("matched_rules")){
										((ObjectNode) event).putArray("matched_rules");
									}
									ArrayNode matchedRules = (ArrayNode) event.get("matched_rules");
									matchedRules.add(rule.get("title").asText());

									//print event
									System.out.println("updated event: " + event);

									//emit for kafka
									out.collect(objectMapper.writeValueAsString(event));

								}
							}

						}


					}

					@Override
					public void processBroadcastElement(String ruleJson, Context ctx, Collector<String> out) throws Exception {
						JsonNode rule = objectMapper.readTree(ruleJson);

						//update the broadcast
						List<JsonNode> rules = new ArrayList<>();
						if(ctx.getBroadcastState(sigmaRuleDescriptor).get(null) != null) {
							rules.addAll(ctx.getBroadcastState(sigmaRuleDescriptor).get(null));
						}
						rules.add(rule);

						ctx.getBroadcastState(sigmaRuleDescriptor).put(null, rules);

					}
				});




//		failureStream.sinkTo(failureSink);

		processedEvents.sinkTo(successSink);


//		env.getConfig().disableGenericTypes();
		// Execute Flink Job
		env.execute("Flink Sigma Rule Processor");
	}


//	CLASS TO COLLECT RULE


	public static boolean matchesSigmaRule(JsonNode event, JsonNode rule) {
		// Check if the logsource matches
		JsonNode eventLogSource = event.path("logsource");
		JsonNode ruleLogSource = rule.path("logsource");

//		System.out.println("event log: " +eventLogSource);

		if (!eventLogSource.path("product").asText().equals(ruleLogSource.path("product").asText())) {
			return false;
		}
		if (!eventLogSource.path("category").asText().equals(ruleLogSource.path("category").asText())) {
			return false;
		}

		// Check the payload against the detection conditions
		JsonNode detection = rule.path("detection");
		JsonNode selection = detection.path("selection_4103");

		String payload = event.path("payload").asText();

		// Check if all required substrings are in the payload
		ArrayNode containsAll = (ArrayNode) selection.path("Payload contains all");
		for (JsonNode required : containsAll) {
			if (!payload.contains(required.asText())) {
				return false;
			}
		}

		// Check if any of the optional substrings are in the payload
		ArrayNode containsAny = (ArrayNode) selection.path("Payload contains");
		boolean anyMatch = false;
		for (JsonNode optional : containsAny) {
			if (payload.contains(optional.asText())) {
				anyMatch = true;
				break;
			}
		}

		System.out.println("Event: " + event.get("id") + " " + anyMatch);

		return anyMatch;
	}
}