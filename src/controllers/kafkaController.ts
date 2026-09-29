import type { Request, RequestHandler, Response } from "express";

import { publishPost } from "../kafkaImplementation.js";

export const showTriggerForm: RequestHandler = (_request, response) => {
  response.render("kafka", { message: null, status: null, submittedText: null });
};

export const triggerKafka: RequestHandler = (request, response, next) => {
  void triggerKafkaRequest(request, response).catch(next);
};

async function triggerKafkaRequest(request: Request, response: Response): Promise<void> {
  const body: unknown = request.body;
  const postText = typeof body === "object" && body !== null && "texts" in body ? body.texts : undefined;
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
