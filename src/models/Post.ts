import mongoose from "mongoose";

interface PostDocument extends mongoose.Document {
  content: string;
  date: string;
}

const postSchema = new mongoose.Schema<PostDocument>({
  content: {
    required: true,
    type: String,
  },
  date: {
    // Mongoose calls this for each document, so creation times are not shared.
    default: () => new Date().toString(),
    type: String,
  },
});

export const Post = mongoose.model<PostDocument>("Posts", postSchema);
