import { Kafka, type EachMessagePayload } from 'kafkajs';
import Sentiment = require('sentiment');

export interface KafkaPost {
	id: string;
	text: string;
	submittedAt: string;
}

export interface SentimentAnalysis {
	score: number;
	comparative: number;
	tokens: string[];
	words: string[];
	positive: string[];
	negative: string[];
	accepted: boolean;
}

export interface AnalyzedPost extends KafkaPost, SentimentAnalysis {}

type ResultHandler = (result: AnalyzedPost) => void | Promise<void>;

// Override these defaults when running against a non-local Kafka cluster.
const topic = process.env.KAFKA_TOPIC || 'posts-to-analyze';
const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092')
	.split(',')
	.map(broker => broker.trim())
	.filter(Boolean);

const kafka = new Kafka({
	clientId: process.env.KAFKA_CLIENT_ID || 'sentiment-analysis',
	brokers
});

function validatePostText(text: unknown): string {
	if (typeof text !== 'string' || text.trim().length === 0) {
		throw new TypeError('Post text must be a non-empty string');
	}

	return text.trim();
}

export function analyzePost(text: unknown): SentimentAnalysis {
	const analysis = new Sentiment().analyze(validatePostText(text));

	return {
		score: analysis.score,
		comparative: analysis.comparative,
		tokens: analysis.tokens,
		words: analysis.words,
		positive: analysis.positive,
		negative: analysis.negative,
		// Keep the Kafka result consistent with the web form's acceptance threshold.
		accepted: analysis.negative.length <= 3
	};
}

export async function publishPost(text: unknown): Promise<KafkaPost> {
	const producer = kafka.producer();
	const post: KafkaPost = {
		id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
		text: validatePostText(text),
		submittedAt: new Date().toISOString()
	};

	let connected = false;
	try {
		await producer.connect();
		connected = true;
		await producer.send({
			topic,
			messages: [{
				key: post.id,
				value: JSON.stringify(post)
			}]
		});
	} finally {
		if (connected) {
			// Release the producer connection even when sending fails.
			await producer.disconnect();
		}
	}

	return post;
}

function parsePost(value: Buffer | null): KafkaPost {
	if (!value) {
		throw new Error('Message has no value');
	}

	const parsed: unknown = JSON.parse(value.toString());
	if (typeof parsed !== 'object' || parsed === null) {
		throw new Error('Message payload must be an object');
	}

	const post = parsed as Record<string, unknown>;
	if (typeof post.id !== 'string' || typeof post.submittedAt !== 'string') {
		throw new Error('Message payload is missing post metadata');
	}

	return {
		id: post.id,
		submittedAt: post.submittedAt,
		text: validatePostText(post.text)
	};
}

async function processMessage(
	{ message }: EachMessagePayload,
	onResult: ResultHandler
): Promise<void> {
	let post: KafkaPost;
	try {
		post = parsePost(message.value);
	} catch (error) {
		// Ignore a malformed event so later valid posts can still be processed.
		console.error('Skipping malformed Kafka message:', getErrorMessage(error));
		return;
	}

	try {
		await onResult({ ...post, ...analyzePost(post.text) });
	} catch (error) {
		console.error('Unable to analyze Kafka message:', getErrorMessage(error));
	}
}

export async function startConsumer(
	onResult: ResultHandler = result => console.log(JSON.stringify(result))
): Promise<() => Promise<void>> {
	const consumer = kafka.consumer({
		groupId: process.env.KAFKA_GROUP_ID || 'sentiment-analysis-workers'
	});

	let connected = false;
	try {
		await consumer.connect();
		connected = true;
		// Replay existing records when this consumer group has no saved offset yet.
		await consumer.subscribe({ topic, fromBeginning: true });
		await consumer.run({
			eachMessage: payload => processMessage(payload, onResult)
		});
	} catch (error) {
		if (connected) {
			await consumer.disconnect().catch(() => undefined);
		}
		throw error;
	}

	let stopped = false;
	return async () => {
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

export async function runCli(): Promise<void> {
	const [role, ...textParts] = process.argv.slice(2);

	if (role === 'producer') {
		const post = await publishPost(textParts.join(' '));
		console.log(`Published post ${post.id} to ${topic}`);
		return;
	}

	if (role === 'consumer') {
		console.log(`Listening for posts on ${topic}`);
		const stop = await startConsumer();
		process.once('SIGINT', () => {
			stop().catch(error => {
				console.error('Unable to stop Kafka consumer:', getErrorMessage(error));
				process.exitCode = 1;
			});
		});
		return;
	}

	throw new Error('Usage: node kafkaImplementation.js <producer "post text"|consumer>');
}