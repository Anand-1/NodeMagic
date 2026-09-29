import { describe, expect, it } from "vitest";

import { analyzePost } from "../kafkaImplementation.js";

describe("analyzePost", () => {
  it("scores text and accepts posts within the negative-word threshold", () => {
    const result = analyzePost("This is a wonderful day");

    expect(result.score).toBeGreaterThan(0);
    expect(result.accepted).toBe(true);
  });

  it("rejects empty input", () => {
    expect(() => analyzePost("  ")).toThrowError(TypeError);
  });
});
