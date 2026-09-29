# sentiment-analysis

![sentiment analysis](/images/sentimental.gif)

Sentiment Analysis api for feelin app

## DESCRIPTION

![sentiment analysis](/images/sentimenatalAnalysis.png)

This project is for sentiment analysis of post/blog submitted by user and then analysising the post and giving it rating . If all goes right then the post/blog submitted by the user will store in mongoDB database otherwise it will show the user to remove abussive words and then resubmit it .

### Setup

Use Node.js 22.12 or newer. Copy `.env.example` to `.env`, then install dependencies:

    npm install

### Run in development

    npm run dev

### Build and run

    npm run build
    npm start

### Frontend

Go to http://127.0.0.1:8000/sentimental
it will throw you a basic form write your post and submit it

This project also uses socket.io show it will show the realtime update of the score and words.

All you can submit your post.

If all goes write (means post is good ) then it will stored in database and give us the response saved. Otherwise it will show the harmful words

## Kafka scenario

The Kafka example publishes submitted post text to `posts-to-analyze`. A consumer scores each post and prints the analysis with an `accepted` flag (posts with more than three negative words are rejected).

The implementation is in `src/kafkaImplementation.ts`. Run `npm run type-check` to validate types.

Start a Kafka broker, then run the consumer in one terminal:

    npm run kafka -- consumer

Publish a post from another terminal:

    npm run kafka -- producer "This is a wonderful day"

Set `KAFKA_BROKERS` to a comma-separated broker list, or override `KAFKA_TOPIC`, `KAFKA_CLIENT_ID`, and `KAFKA_GROUP_ID` as needed. Defaults are `localhost:9092`, `posts-to-analyze`, `sentiment-analysis`, and `sentiment-analysis-workers`.

### Quality checks

    npm run build
    npm run type-check
    npm run lint
    npm run test:run

## LIMITATIONS

### Maybe

one language is supported at a time.
If add our own language then we need to define every Words and its score

### TODO

improved the logic of sentiment analysis in if block
