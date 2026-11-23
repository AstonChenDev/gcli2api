// Sanitized local calibration outcome; contains no account identifiers or credentials.
export const quotaCalibration = {
  "testedAt": null,
  "attemptedAt": "2026-11-16T10:51+08:00",
  "status": "inconclusive",
  "tier": "Ultra",
  "unit": "weekly_percentage_points_per_million_tokens",
  "rules": {},
  "message": "隔离测试期间，额度读数回跳，且一次成功调用后周额度余额反而增加 0.037094 个百分点；未跨重置时间、未发现同期业务调用。无法可靠归因，未应用实测倍率。当前仍可使用等权 Token 或自定义权重估算。",
  "observations": [
    {
      "model": "gemini-4-argon-medium",
      "profile": "输入较多",
      "input": 3108,
      "output": 512,
      "generationSucceeded": true,
      "accepted": false,
      "reason": "读数回跳，样本不采用"
    },
    {
      "model": "gemini-4-argon-medium",
      "profile": "输入较多",
      "input": 3102,
      "output": 256,
      "generationSucceeded": true,
      "accepted": false,
      "reason": "读数回跳，样本不采用"
    },
    {
      "model": "gemini-4-argon-medium",
      "profile": "输入较多",
      "input": 3105,
      "output": 128,
      "generationSucceeded": true,
      "accepted": false,
      "reason": "配对样本异常，无法校准"
    },
    {
      "model": "gemini-4-argon-medium",
      "profile": "输出较多",
      "input": 257,
      "output": 512,
      "generationSucceeded": true,
      "accepted": false,
      "reason": "余额反增，样本不采用"
    }
  ]
};
