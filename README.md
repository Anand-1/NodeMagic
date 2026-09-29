# Sentiment Analysis

![Sentiment analysis demo](/images/sentimental.gif)

An Express application for analyzing submitted text. The browser displays live sentiment results through Socket.IO. On submission, text with no more than three negative words is saved to MongoDB; otherwise, the result page reports that it did not meet the current threshold.

## Requirements

- Node.js 22.12 or newer
- MongoDB for saving submitted posts
- A Kafka broker only if using the Kafka producer or consumer

## Setup

Install dependencies and create a local environment file:

```sh
npm install
cp .env.example .env
```

Set `MONGO_URL` in `.env` to your MongoDB connection string. The server can start without MongoDB, but saving a post will fail until the database is reachable.

## Run

Start the development server with automatic reload:

```sh
npm run dev
```

Open [http://localhost:8000/sentimental](http://localhost:8000/sentimental). The port can be changed with `PORT` in `.env`.

Build and start the compiled app:

```sh
npm run build
npm start
```

## Kafka

Start a Kafka broker, then run the consumer:

```sh
npm run kafka -- consumer
```

Publish a post from another terminal:

```sh
npm run kafka -- producer "This is a wonderful day"
```

The consumer analyzes messages from the configured topic and prints each result with an `accepted` flag. A post is accepted when it has at most three negative words.

| Variable          | Default                      | Purpose                          |
| ----------------- | ---------------------------- | -------------------------------- |
| `KAFKA_BROKERS`   | `localhost:9092`             | Comma-separated broker addresses |
| `KAFKA_TOPIC`     | `posts-to-analyze`           | Topic used for posts             |
| `KAFKA_CLIENT_ID` | `sentiment-analysis`         | Kafka client identifier          |
| `KAFKA_GROUP_ID`  | `sentiment-analysis-workers` | Consumer group identifier        |

## Quality Checks

```sh
npm run build
npm run type-check
npm run lint
npm run format:check
npm run test:run
```

## Project Layout

- `src/index.ts`: Express and Socket.IO server setup
- `src/controllers/`: Form and Kafka request handlers
- `src/routes/`: HTTP routes
- `src/models/`: MongoDB post model
- `src/kafkaImplementation.ts`: Kafka producer, consumer, and sentiment analysis
- `views/` and `public/`: EJS templates and static assets
