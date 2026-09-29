import type { Request, RequestHandler, Response } from "express";
import type { ParamsDictionary } from "express-serve-static-core";

import Sentiment from "sentiment";

import { Post } from "../models/Post.js";
import { getSubmittedText } from "../utils/submittedText.js";

type FormHandler = RequestHandler<ParamsDictionary, unknown, unknown>;
type FormRequest = Request<ParamsDictionary, unknown, unknown>;
type FormResponse = Response<unknown>;

export const renderSentimentForm: RequestHandler = (_request, response) => {
  response.render("form", { msg: "somethingto test" });
};

export const analyzeAndSavePost: FormHandler = (request, response, next) => {
  void analyzeAndSavePostRequest(request, response).catch(next);
};

async function analyzeAndSavePostRequest(request: FormRequest, response: FormResponse): Promise<void> {
  const postText = getSubmittedText(request.body);
  if (postText === undefined) {
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
    // Persist only posts that pass the negative-word threshold shown in the response.
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
    // Convert database failures into a service response instead of an unhandled rejection.
    console.error("Unable to save post:", error);
    response.status(503).render("sucess", { msg: "Unable to save the post. Check the database connection." });
  }
}
