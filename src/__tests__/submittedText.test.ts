import { describe, expect, it } from "vitest";

import { getSubmittedText } from "../utils/submittedText.js";

describe("getSubmittedText", () => {
  it("returns non-empty submitted text", () => {
    expect(getSubmittedText({ texts: "A clear post" })).toBe("A clear post");
  });

  it("rejects missing, non-string, and blank values", () => {
    expect(getSubmittedText(null)).toBeUndefined();
    expect(getSubmittedText({})).toBeUndefined();
    expect(getSubmittedText({ texts: 42 })).toBeUndefined();
    expect(getSubmittedText({ texts: "  " })).toBeUndefined();
  });
});
