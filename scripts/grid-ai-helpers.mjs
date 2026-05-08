const GEMINI_API_KEY = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY || '';
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || '';
const DEEPSEEK_MODEL = process.env.DEEPSEEK_MODEL || 'deepseek-v4-flash';
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY || '';
const OPENROUTER_MODELS = (process.env.OPENROUTER_MODELS || 'google/gemini-2.5-flash,openai/gpt-4.1-mini,deepseek/deepseek-chat-v3.1')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const GEMINI_MODELS = (process.env.GEMINI_MODELS || 'gemini-2.5-flash-lite,gemini-2.5-flash,gemini-flash-latest')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const HTTP_TIMEOUT_MS = Math.max(5000, Number(process.env.GRID_AI_HTTP_TIMEOUT_MS || 45000));
const AI_TIMEOUT_MS = Math.max(8000, Number(process.env.GRID_AI_TIMEOUT_MS || 60000));
const GRID_AI_MAX_CHARS = Math.max(3000, Number(process.env.GRID_AI_MAX_CHARS || 10000));
const GRID_AI_REQUEST_DELAY_MS = Math.max(0, Number(process.env.GRID_AI_REQUEST_DELAY_MS || 500));

export const SIX_M_OPTIONS = ['Manpower', 'Method', 'Material', 'Machine', 'Money', 'Market'];
const SIX_M_SIGNAL_MAP = {
  Manpower: /\b(training|trainings|trainer|trainers|capacity building|skill building|workshop|workshops|awareness camp|sensiti[sz]ation)\b/i,
  Method: /\b(consulting|consultancy|consultant|mentoring|mentor|technology transfer|process|processes|workflow|protocol|sop|sops|standard operating procedure|manual|manuals|blog|blogs|video|videos|guide|guides|how to)\b/i,
  Material: /\b(raw material|raw materials|material supply|supply of materials|input supply|feedstock|seed supply|sapling supply)\b/i,
  Machine: /\b(machine|machines|machinery|equipment|tool|tools|plant setup|plant installation|processing unit|device|devices|unit setup)\b/i,
  Money: /\b(financial support|funding support|grant|grants|loan|loans|credit support|working capital|subsidy|subsidies|investment support|finance|financing)\b/i,
  Market: /\b(product purchase|material purchase|procurement|market support|market linkage|market linkages|market report|buyer support|sales channel|distribution support|value chain|marketing)\b/i,
};

function requireString(value) {
  return typeof value === 'string' ? value.trim() : '';
}

export function cleanText(value) {
  return requireString(value).replace(/\s+/g, ' ').trim();
}

function dedupe(values) {
  return [...new Set((values || []).map((value) => cleanText(value)).filter(Boolean))];
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function stripCodeFences(value) {
  return String(value || '').replace(/^```(?:json)?/i, '').replace(/```$/i, '').trim();
}

function parseJsonObject(text) {
  const cleaned = stripCodeFences(text);
  try {
    return JSON.parse(cleaned);
  } catch {
    const match = cleaned.match(/\{[\s\S]*\}/);
    if (!match) throw new Error('AI response did not contain valid JSON.');
    return JSON.parse(match[0]);
  }
}

function normalizeTagList(values) {
  return dedupe(Array.isArray(values) ? values : []).slice(0, 18);
}

function normalizeStepList(values) {
  return dedupe(Array.isArray(values) ? values : []).slice(0, 12);
}

function normalizeSixM(values) {
  const normalizedMap = new Map(SIX_M_OPTIONS.map((item) => [item.toLowerCase(), item]));
  return dedupe(Array.isArray(values) ? values : [])
    .map((item) => normalizedMap.get(cleanText(item).toLowerCase()) || '')
    .filter(Boolean)
    .slice(0, 6);
}

function inferSixMFromText(text) {
  const signals = cleanText(text);
  return SIX_M_OPTIONS.filter((item) => SIX_M_SIGNAL_MAP[item].test(signals));
}

function normalizeAiPayload(parsed, fallbackText) {
  const normalized = {
    practice_name: cleanText(parsed.practice_name) || null,
    innovator_name: cleanText(parsed.innovator_name) || null,
    summary_of_practice: cleanText(parsed.summary_of_practice) || null,
    process_steps: normalizeStepList(parsed.process_steps),
    six_m_categories: normalizeSixM(parsed.six_m_categories),
    tags: normalizeTagList(parsed.tags),
  };
  if (!normalized.six_m_categories.length) {
    normalized.six_m_categories = inferSixMFromText(fallbackText);
  }
  return normalized;
}

function extractTextFromResponse(response) {
  const data = response || {};
  const candidates = [
    data?.candidates?.[0]?.content?.parts?.[0]?.text,
    data?.choices?.[0]?.message?.content,
    data?.choices?.[0]?.text,
    data?.output?.[0]?.content?.[0]?.text,
  ];
  for (const candidate of candidates) {
    if (typeof candidate === 'string' && candidate.trim()) return candidate.trim();
    if (Array.isArray(candidate)) {
      const joined = candidate.map((item) => typeof item?.text === 'string' ? item.text : '').filter(Boolean).join('\n').trim();
      if (joined) return joined;
    }
  }
  return '';
}

function isTransientError(status, raw) {
  const text = String(raw || '');
  return status === 429 || status === 500 || status === 503 || /temporar|timeout|overload|busy|rate.?limit|UNAVAILABLE|RESOURCE_EXHAUSTED/i.test(text);
}

async function fetchWithTimeout(url, options = {}, timeoutMs = HTTP_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(`Timed out after ${timeoutMs}ms`), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

function buildPrompt(record) {
  return [
    'Classify this GRID practice for an admin-reviewed innovation directory.',
    'Return strict JSON only.',
    'Schema:',
    '{"practice_name":string|null,"innovator_name":string|null,"summary_of_practice":string|null,"process_steps":string[],"six_m_categories":string[],"tags":string[]}',
    'Rules:',
    '- Use null when a value is not reliably supported by the source.',
    '- six_m_categories must only use: Manpower, Method, Material, Machine, Money, Market.',
    '- Apply 6M strictly using these meanings:',
    '- Manpower = trainings or capacity-building support.',
    '- Method = consulting, mentoring, technology transfer, processes, videos, SOPs, manuals, or blogs.',
    '- Market = product/material purchase, market support, or market reports.',
    '- Material = raw material supply.',
    '- Machine = machinery or plant setup.',
    '- Money = financial support.',
    '- Do not assign a 6M category unless the source clearly supports that meaning.',
    '- tags should be short, admin-friendly, and useful in search.',
    '- summary_of_practice should be concise, specific, and suitable for an admin-reviewed directory.',
    '- process_steps should only be included when the source clearly describes a sequence, workflow, or method.',
    `Current record:\n${JSON.stringify(record)}`,
  ].join('\n');
}

async function requestGemini(prompt) {
  let lastError = null;
  for (const model of GEMINI_MODELS) {
    for (let attempt = 0; attempt < 3; attempt += 1) {
      if (attempt > 0) await sleep(1000 * attempt);
      const response = await fetchWithTimeout(`https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{ role: 'user', parts: [{ text: prompt }] }],
          generationConfig: {
            temperature: 0.1,
            responseMimeType: 'application/json',
          },
        }),
      }, AI_TIMEOUT_MS);
      if (!response.ok) {
        const raw = await response.text().catch(() => '');
        lastError = new Error(raw || `Gemini request failed (${response.status})`);
        if (isTransientError(response.status, raw)) continue;
        break;
      }
      const data = await response.json();
      const text = extractTextFromResponse(data);
      return { model, text };
    }
  }
  throw lastError || new Error('Gemini classification failed.');
}

async function requestDeepSeek(prompt) {
  let lastError = null;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    if (attempt > 0) await sleep(1000 * attempt);
    const response = await fetchWithTimeout('https://api.deepseek.com/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${DEEPSEEK_API_KEY}`,
      },
      body: JSON.stringify({
        model: DEEPSEEK_MODEL,
        temperature: 0.1,
        response_format: { type: 'json_object' },
        messages: [
          { role: 'system', content: 'You classify GRID innovation practices and return strict JSON only.' },
          { role: 'user', content: prompt },
        ],
      }),
    }, AI_TIMEOUT_MS);
    if (!response.ok) {
      const raw = await response.text().catch(() => '');
      lastError = new Error(raw || `DeepSeek request failed (${response.status})`);
      if (isTransientError(response.status, raw)) continue;
      break;
    }
    const data = await response.json();
    const text = extractTextFromResponse(data);
    return { model: DEEPSEEK_MODEL, text };
  }
  throw lastError || new Error('DeepSeek classification failed.');
}

async function requestOpenRouter(prompt) {
  let lastError = null;
  for (const model of OPENROUTER_MODELS) {
    for (let attempt = 0; attempt < 3; attempt += 1) {
      if (attempt > 0) await sleep(1000 * attempt);
      const response = await fetchWithTimeout('https://openrouter.ai/api/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${OPENROUTER_API_KEY}`,
          'HTTP-Referer': 'https://github.com/tanmaymukherji/grid-innovation-directory',
          'X-Title': 'GRID Innovation Directory',
        },
        body: JSON.stringify({
          model,
          temperature: 0.1,
          response_format: { type: 'json_object' },
          messages: [
            { role: 'system', content: 'You classify GRID innovation practices and return strict JSON only.' },
            { role: 'user', content: prompt },
          ],
        }),
      }, AI_TIMEOUT_MS);
      if (!response.ok) {
        const raw = await response.text().catch(() => '');
        lastError = new Error(raw || `OpenRouter request failed (${response.status})`);
        if (isTransientError(response.status, raw)) continue;
        break;
      }
      const data = await response.json();
      const text = extractTextFromResponse(data);
      return { model, text };
    }
  }
  throw lastError || new Error('OpenRouter classification failed.');
}

export function hasAiProviderConfigured() {
  return Boolean(GEMINI_API_KEY || DEEPSEEK_API_KEY || OPENROUTER_API_KEY);
}

export function buildGridAiContext(record) {
  return {
    portal_product_id: record.portal_product_id || record.practiceId || null,
    practice_name: record.product_name || record.title || null,
    innovator_name: record.vendor_name || record.innovatorName || null,
    practice_summary: record.practice_summary || record.summary || record.product_description || null,
    problem_statement: record.problemStatement || null,
    product_categories: record.product_categories || record.categories || [],
    source_tags: record.tags || [],
    product_location_text: record.product_location_text || record.location || null,
    innovator_details: record.innovator_details || record.innovatorDetails || null,
    practice_details: record.practice_details || record.practiceDetails || null,
    source_reference: record.source_reference || record.referenceText || null,
    raw_product: record.raw_product || null,
  };
}

export async function enrichGridPractice(record) {
  const context = buildGridAiContext(record);
  const fallbackText = [
    context.practice_name,
    context.innovator_name,
    context.practice_summary,
    context.problem_statement,
    context.product_location_text,
    ...(context.product_categories || []),
    ...(context.source_tags || []),
    context.innovator_details,
    context.practice_details,
    context.source_reference,
  ].filter(Boolean).join(' ').slice(0, GRID_AI_MAX_CHARS);
  const prompt = buildPrompt({
    ...context,
    practice_details: cleanText(context.practice_details).slice(0, GRID_AI_MAX_CHARS),
    innovator_details: cleanText(context.innovator_details).slice(0, Math.floor(GRID_AI_MAX_CHARS / 2)),
  });

  let response;
  if (OPENROUTER_API_KEY) response = await requestOpenRouter(prompt);
  else if (GEMINI_API_KEY) response = await requestGemini(prompt);
  else if (DEEPSEEK_API_KEY) response = await requestDeepSeek(prompt);
  else throw new Error('No AI provider configured for GRID enrichment.');

  if (GRID_AI_REQUEST_DELAY_MS > 0) await sleep(GRID_AI_REQUEST_DELAY_MS);
  const parsed = parseJsonObject(response.text);
  return {
    aiModel: response.model,
    aiSummary: normalizeAiPayload(parsed, fallbackText),
  };
}
