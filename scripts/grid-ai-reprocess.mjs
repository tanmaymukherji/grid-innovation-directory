import { createHash } from "node:crypto";
import { buildGridAiContext, enrichGridPractice, hasAiProviderConfigured } from "./grid-ai-helpers.mjs";

const SUPABASE_URL = process.env.GRID_SUPABASE_URL || "https://zphabezqbboaexmmhcic.supabase.co";
const SUPABASE_SERVICE_ROLE_KEY = process.env.GRID_SUPABASE_SERVICE_ROLE_KEY || "";
const REPROCESS_LIMIT = Math.max(1, Number(process.env.GRID_REPROCESS_LIMIT || 200));
const OFFSET = Math.max(0, Number(process.env.GRID_REPROCESS_OFFSET || 0));
const ONLY_PRODUCT_ID = String(process.env.GRID_ONLY_PRODUCT_ID || "").trim();

if (!SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing GRID_SUPABASE_SERVICE_ROLE_KEY.");
  process.exit(1);
}

if (!hasAiProviderConfigured()) {
  console.error("No AI provider configured. Set GEMINI_API_KEY, GOOGLE_API_KEY, DEEPSEEK_API_KEY, or OPENROUTER_API_KEY.");
  process.exit(1);
}

async function fetchRows() {
  const select = [
    "portal_product_id",
    "vendor_name",
    "product_name",
    "product_description",
    "product_link",
    "product_location_text",
    "product_categories",
    "practice_summary",
    "innovator_details",
    "practice_details",
    "source_reference",
    "tags",
    "raw_product",
  ].join(",");
  const filter = ONLY_PRODUCT_ID
    ? `&portal_product_id=eq.${encodeURIComponent(ONLY_PRODUCT_ID)}`
    : `&order=product_name.asc&offset=${OFFSET}&limit=${REPROCESS_LIMIT}`;
  const url = `${SUPABASE_URL}/rest/v1/grid_practices?select=${encodeURIComponent(select)}${filter}`;
  const response = await fetch(url, {
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    },
  });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(text || `grid_practices fetch failed (${response.status})`);
  }
  const data = await response.json();
  return Array.isArray(data) ? data : [];
}

function buildAiSourceHash(row) {
  return createHash("sha256").update(JSON.stringify({
    portal_product_id: row.portal_product_id,
    product_name: row.product_name,
    vendor_name: row.vendor_name,
    practice_summary: row.practice_summary,
    product_description: row.product_description,
    product_location_text: row.product_location_text,
    product_categories: row.product_categories,
    innovator_details: row.innovator_details,
    practice_details: row.practice_details,
    source_reference: row.source_reference,
    raw_product: row.raw_product,
  })).digest("hex");
}

async function updateRow(row, aiModel, aiSummary) {
  const response = await fetch(`${SUPABASE_URL}/rest/v1/grid_practices?portal_product_id=eq.${encodeURIComponent(row.portal_product_id)}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      Prefer: "return=minimal",
    },
    body: JSON.stringify({
      ai_model: aiModel,
      ai_summary: aiSummary,
      ai_classified_at: new Date().toISOString(),
      ai_source_hash: buildAiSourceHash(row),
      updated_at: new Date().toISOString(),
    }),
  });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(text || `grid_practices update failed (${response.status})`);
  }
}

async function main() {
  const rows = await fetchRows();
  console.log(`Loaded ${rows.length} practices for AI reprocessing.`);
  for (const row of rows) {
    console.log(`AI reprocess: ${row.product_name} (${row.portal_product_id})`);
    const { aiModel, aiSummary } = await enrichGridPractice(buildGridAiContext(row));
    await updateRow(row, aiModel, aiSummary);
  }
  console.log("GRID AI reprocessing complete.");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
