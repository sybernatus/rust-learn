use clap::{App, Arg};

use rdkafka::{ClientConfig as KafkaClientConfig, ClientContext, Message, TopicPartitionList};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::util::get_rdkafka_version;

type LoggingConsumer = StreamConsumer<CustomContext>;

pub struct Kafka {
	client: LoggingConsumer
}

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
	fn pre_rebalance(&self, rebalance: &Rebalance) {
		println!("Pre rebalance {:?}", rebalance);
	}

	fn post_rebalance(&self, rebalance: &Rebalance) {
		println!("Post rebalance {:?}", rebalance);
	}

	fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
		println!("Committing offsets: {:?}", result);
	}
}

impl Kafka {
	pub fn init(brokers: &str, group_id: &str) -> Self {
		Self {
			client: KafkaClientConfig::new()
				.set("group.id", group_id)
				.set("bootstrap.servers", brokers)
				.set("enable.partition.eof", "false")
				.set("session.timeout.ms", "6000")
				.set("enable.auto.commit", "false")
				.set("statistics.interval.ms", "30000")
				.set("auto.offset.reset", "smallest")
				.set_log_level(RDKafkaLogLevel::Debug)
				.create_with_context(CustomContext)
				.expect("Consumer creation failed")
		}
	}

	pub async fn consume_and_print(self, topics: &[&str]) {
		self.client
			.subscribe(&topics.to_vec())
			.expect("Can't subscribe to specified topics");

		loop {
			match self.client.recv().await {
				Err(e) => println!("Kafka error: {}", e),
				Ok(message) => {
					let payload = match message.payload_view::<str>() {
						None => "",
						Some(Ok(s)) => s,
						Some(Err(e)) => {
							println!("Error while deserializing message payload: {:?}", e);
							""
						}
					};
					println!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
									 message.key(), payload, message.topic(), message.partition(), message.offset(), message.timestamp());
					self.client.commit_message(&message, CommitMode::Async).unwrap();
				}
			};
		}
	}
}

#[tokio::main]
async fn main() {
	let matches = App::new("consumer")
		.arg(
			Arg::with_name("brokers")
				.short("b")
				.long("brokers")
				.takes_value(true)
				.default_value("localhost:9092")
		)
		.arg(
			Arg::with_name("group-id")
				.short("g")
				.long("group-id")
				.takes_value(true)
				.default_value("test")
		)
		.arg(
			Arg::with_name("topics")
				.short("t")
				.long("topics")
				.takes_value(true)
				.default_value("topic_test")
		)
		.get_matches();

	let (version_n, version_s) = get_rdkafka_version();
	println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);
	let topics = matches.values_of("topics").unwrap().collect::<Vec<&str>>();
	let brokers = matches.value_of("brokers").unwrap();
	let group_id = matches.value_of("group-id").unwrap();

	Kafka::init(brokers, group_id)
		.consume_and_print(&topics).await
}

