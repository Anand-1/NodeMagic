import type { Request, RequestHandler, Response } from "express";
import type { ParamsDictionary } from "express-serve-static-core";

import { publishPost } from "../kafkaImplementation.js";
import { getSubmittedText } from "../utils/submittedText.js";

type KafkaHandler = RequestHandler<ParamsDictionary, unknown, unknown>;
type KafkaRequest = Request<ParamsDictionary, unknown, unknown>;
type KafkaResponse = Response<unknown>;

export const renderKafkaTriggerForm: RequestHandler = (_request, response) => {
  response.render("kafka", { message: null, status: null, submittedText: null });
};

export const publishPostToKafka: KafkaHandler = (request, response, next) => {
  void publishPostToKafkaRequest(request, response).catch(next);
};

async function publishPostToKafkaRequest(request: KafkaRequest, response: KafkaResponse): Promise<void> {
  const postText = getSubmittedText(request.body);
  try {
    const post = await publishPost(postText);
    response.render("kafka", {
      message: `Post ${post.id} was published to Kafka.`,
      status: "success",
      submittedText: post.text,
    });
  } catch (error) {
    const isInvalidPost = error instanceof TypeError;
    if (!isInvalidPost) {
      console.error("Kafka publish failed:", error instanceof Error ? error.message : String(error));
    }

    response.status(isInvalidPost ? 400 : 503).render("kafka", {
      message: isInvalidPost ? error.message : "Kafka is unavailable. Check the broker settings and try again.",
      status: "error",
      submittedText: null,
    });
  }
}
