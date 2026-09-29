import { type EachMessagePayload, Kafka } from "kafkajs";
import { pathToFileURL } from "node:url";
import Sentiment from "sentiment";

export interface AnalyzedPost extends KafkaPost, SentimentAnalysis {}

export interface KafkaPost {
  id: string;
  submittedAt: string;
  text: string;
}

export interface SentimentAnalysis {
  accepted: boolean;
  comparative: number;
  negative: string[];
  positive: string[];
  score: number;
  tokens: string[];
  words: string[];
}

type ResultHandler = (result: AnalyzedPost) => Promise<void> | void;

const topic = process.env.KAFKA_TOPIC ?? "posts-to-analyze";
const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);

const kafka = new Kafka({
  brokers,
  clientId: process.env.KAFKA_CLIENT_ID ?? "sentiment-analysis",
});

export function analyzePost(text: unknown): SentimentAnalysis {
  const analysis = new Sentiment().analyze(validatePostText(text));

  return {
    accepted: analysis.negative.length <= 3,
    comparative: analysis.comparative,
    negative: analysis.negative,
    positive: analysis.positive,
    score: analysis.score,
    tokens: analysis.tokens,
    words: analysis.words,
  };
}

export async function publishPost(text: unknown): Promise<KafkaPost> {
  const producer = kafka.producer();
  const post: KafkaPost = {
    id: `${Date.now().toString()}-${Math.random().toString(36).slice(2)}`,
    submittedAt: new Date().toISOString(),
    text: validatePostText(text),
  };

  let connected = false;
  try {
    await producer.connect();
    connected = true;
    await producer.send({
      messages: [{ key: post.id, value: JSON.stringify(post) }],
      topic,
    });
  } finally {
    if (connected) {
      // Release the connection even when the broker rejects the send.
      await producer.disconnect();
    }
  }

  return post;
}

export async function runCli(): Promise<void> {
  const [role, ...textParts] = process.argv.slice(2);

  if (role === "producer") {
    const post = await publishPost(textParts.join(" "));
    console.log(`Published post ${post.id} to ${topic}`);
    return;
  }

  if (role === "consumer") {
    console.log(`Listening for posts on ${topic}`);
    const stop = await startConsumer();
    process.once("SIGINT", () => {
      stop().catch((error: unknown) => {
        console.error("Unable to stop Kafka consumer:", getErrorMessage(error));
        process.exitCode = 1;
      });
    });
    return;
  }

  throw new Error('Usage: npm run kafka:run -- <producer "post text"|consumer>');
}

export async function startConsumer(
  onResult: ResultHandler = (result) => {
    console.log(JSON.stringify(result));
  },
): Promise<() => Promise<void>> {
  const consumer = kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID ?? "sentiment-analysis-workers",
  });

  let connected = false;
  try {
    await consumer.connect();
    connected = true;
    // Replay existing records when this consumer group has no saved offset yet.
    await consumer.subscribe({ fromBeginning: true, topic });
    await consumer.run({
      eachMessage: (payload) => processMessage(payload, onResult),
    });
  } catch (error) {
    if (connected) {
      await consumer.disconnect().catch(() => undefined);
    }
    throw error;
  }

  let stopped = false;
  return async () => {
    // Shutdown may be requested more than once, such as during repeated signals.
    if (stopped) {
      return;
    }
    stopped = true;
    await consumer.disconnect();
  };
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function parsePost(value: Buffer | null): KafkaPost {
  if (!value) {
    throw new Error("Message has no value");
  }

  const parsed: unknown = JSON.parse(value.toString());
  if (typeof parsed !== "object" || parsed === null) {
    throw new Error("Message payload must be an object");
  }

  const post = parsed as Record<string, unknown>;
  // Reject malformed events individually so they do not stop later messages in the partition.
  if (typeof post.id !== "string" || typeof post.submittedAt !== "string") {
    throw new Error("Message payload is missing post metadata");
  }

  return {
    id: post.id,
    submittedAt: post.submittedAt,
    text: validatePostText(post.text),
  };
}

async function processMessage({ message }: EachMessagePayload, onResult: ResultHandler): Promise<void> {
  let post: KafkaPost;
  try {
    post = parsePost(message.value);
  } catch (error) {
    console.error("Skipping malformed Kafka message:", getErrorMessage(error));
    return;
  }

  try {
    await onResult({ ...post, ...analyzePost(post.text) });
  } catch (error) {
    console.error("Unable to analyze Kafka message:", getErrorMessage(error));
  }
}

function validatePostText(text: unknown): string {
  if (typeof text !== "string" || text.trim().length === 0) {
    throw new TypeError("Post text must be a non-empty string");
  }

  return text.trim();
}

// Run the producer/consumer CLI only when this module is the process entry point.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  runCli().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
