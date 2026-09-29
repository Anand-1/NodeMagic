import type { Request, RequestHandler, Response } from "express";

import Sentiment from "sentiment";

import { Post } from "../models/Post.js";

export const handleSubmit: RequestHandler = (_request, response) => {
  response.render("form", { msg: "somethingto test" });
};

export const submitPost: RequestHandler = (request, response, next) => {
  void submitPostRequest(request, response).catch(next);
};

async function submitPostRequest(request: Request, response: Response): Promise<void> {
  const body: unknown = request.body;
  const postText: unknown = typeof body === "object" && body !== null && "texts" in body ? body.texts : undefined;
  if (typeof postText !== "string" || postText.trim().length === 0) {
    response.render("sucess", { msg: "please type anything" });
    return;
  }

  const result = new Sentiment().analyze(postText);
  const { comparative, negative, positive, score, tokens, words } = result;
  if (negative.length > 3) {
    response.render("sucess", {
      comparative,
      msg: "Words are not above our quality standard",
      negative,
      positive,
      score,
      tokens,
      words,
    });
    return;
  }

  try {
    await Post.create({ content: postText });
    response.render("sucess", {
      comparative,
      msg: "Saved...",
      negative,
      positive,
      score,
      tokens,
      words,
    });
  } catch (error) {
    console.error("Unable to save post:", error);
    response.status(503).render("sucess", { msg: "Unable to save the post. Check the database connection." });
  }
}
