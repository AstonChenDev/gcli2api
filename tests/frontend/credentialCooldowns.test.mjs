import assert from "node:assert/strict";
import test from "node:test";
import { credentialQuotaCooldowns, formatCooldownRemaining } from "../../frontend/src/utils/credentialCooldowns.js";

const now = 1_795_600_000;

test("image-only cooldown leaves the text cooldown clear", () => {
  const image = now + 5 * 86400;
  assert.deepEqual(credentialQuotaCooldowns({ image }, now), { text: [], image });
});

test("text families keep their own deadlines independently of the image deadline", () => {
  const cooldowns = { gemini: now + 120, argon: now + 45, claude_gpt: now + 3600, image: now + 86400 };
  assert.deepEqual(credentialQuotaCooldowns(cooldowns, now), {
    text: [
      { group: "gemini", label: "Gemini", until: now + 120 },
      { group: "argon", label: "Argon", until: now + 45 },
      { group: "claude_gpt", label: "Claude / GPT", until: now + 3600 },
    ],
    image: now + 86400,
  });
});

test("expired cooldowns clear independently", () => {
  const text = [{ group: "gemini", label: "Gemini", until: now + 10 }];
  assert.deepEqual(credentialQuotaCooldowns({ gemini: now + 10, image: now }, now), { text, image: null });
  assert.deepEqual(credentialQuotaCooldowns({ gemini: now, image: now - 1 }, now), { text: [], image: null });
});

test("remaining time is readable from subsecond to multiday windows", () => {
  assert.equal(formatCooldownRemaining(now + 0.25, now), "1秒");
  assert.equal(formatCooldownRemaining(now + 120, now), "2分 0秒");
  assert.equal(formatCooldownRemaining(now + 5400, now), "1小时 30分");
  assert.equal(formatCooldownRemaining(now + 5 * 86400 + 3600, now), "5天 1小时");
});
