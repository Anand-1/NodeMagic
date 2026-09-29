import express from "express";
import mongoose from "mongoose";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Sentiment from "sentiment";
import socketIo from "socket.io";

import { sentimentAndKafkaRouter } from "./routes/form.js";

// Resolve templates and static assets from the project root in both src and dist runs.
const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const app = express();
const server = http.createServer(app);
const io = socketIo(server);
const port = Number(process.env.PORT ?? 8000);

app.set("view engine", "ejs");
app.set("views", path.join(projectRoot, "views"));
app.use(express.static(path.join(projectRoot, "public")));
app.use(express.urlencoded({ extended: false }));
app.use(sentimentAndKafkaRouter);

io.on("connection", (socket) => {
  socket.on("runanaysis", (text: unknown) => {
    // Socket payloads come from the browser, so validate their runtime type before analysis.
    socket.emit("result", new Sentiment().analyze(typeof text === "string" ? text : ""));
  });
});

const mongoUrl = process.env.MONGO_URL;
if (mongoUrl) {
  // Keep HTTP available during database outages; persistence handlers report failed saves.
  void mongoose.connect(mongoUrl).catch((error: unknown) => {
    console.error("MongoDB connection failed:", error);
  });
}

server.listen(port, () => {
  console.log(`Sentiment app listening on port ${port.toString()}`);
});
