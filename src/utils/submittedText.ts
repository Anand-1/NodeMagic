export function getSubmittedText(requestBody: unknown): string | undefined {
  // URL-encoded parsing does not guarantee a valid field or string value.
  if (typeof requestBody !== "object" || requestBody === null || !("texts" in requestBody)) {
    return undefined;
  }

  const submittedText = requestBody.texts;
  if (typeof submittedText !== "string" || submittedText.trim().length === 0) {
    return undefined;
  }

  return submittedText;
}
