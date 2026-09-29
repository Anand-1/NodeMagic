import express from "express";
import mongoose from "mongoose";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Sentiment from "sentiment";
import socketIo from "socket.io";

import { formRouter } from "./routes/form.js";

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const app = express();
const server = http.createServer(app);
const io = socketIo(server);
const port = Number(process.env.PORT ?? 8000);

app.set("view engine", "ejs");
app.set("views", path.join(projectRoot, "views"));
app.use(express.static(path.join(projectRoot, "public")));
app.use(express.urlencoded({ extended: false }));
app.use(formRouter);

io.on("connection", (socket) => {
  socket.on("runanaysis", (text: unknown) => {
    socket.emit("result", new Sentiment().analyze(typeof text === "string" ? text : ""));
  });
});

const mongoUrl = process.env.MONGO_URL;
if (mongoUrl) {
  void mongoose.connect(mongoUrl).catch((error: unknown) => {
    console.error("MongoDB connection failed:", error);
  });
}

server.listen(port, () => {
  console.log(`Sentiment app listening on port ${port.toString()}`);
});
