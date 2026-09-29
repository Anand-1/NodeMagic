import express from "express";

import { handleSubmit, submitPost } from "../controllers/formController.js";
import { showTriggerForm, triggerKafka } from "../controllers/kafkaController.js";

export const formRouter = express.Router();

formRouter.get("/kafka-trigger", showTriggerForm);
formRouter.post("/kafka-trigger", triggerKafka);
formRouter.get("/sentimental", handleSubmit);
formRouter.post("/sentimental", submitPost);
