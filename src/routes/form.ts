import express from "express";

import { analyzeAndSavePost, renderSentimentForm } from "../controllers/formController.js";
import { publishPostToKafka, renderKafkaTriggerForm } from "../controllers/kafkaController.js";

// Keep the sentiment form and Kafka trigger endpoints together for app registration.
export const sentimentAndKafkaRouter = express.Router();

sentimentAndKafkaRouter.get("/kafka-trigger", renderKafkaTriggerForm);
sentimentAndKafkaRouter.post("/kafka-trigger", publishPostToKafka);
sentimentAndKafkaRouter.get("/sentimental", renderSentimentForm);
sentimentAndKafkaRouter.post("/sentimental", analyzeAndSavePost);
