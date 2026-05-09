import { createHash } from "node:crypto";
import { buildGridAiContext, enrichGridPractice, hasAiProviderConfigured } from "./grid-ai-helpers.mjs";

const BASE_URL = "https://grid.gian.org.in";
const SUPABASE_URL = process.env.GRID_SUPABASE_URL || "https://zphabezqbboaexmmhcic.supabase.co";
const FUNCTION_URL = process.env.GRID_FUNCTION_URL || "https://zphabezqbboaexmmhcic.supabase.co/functions/v1/grid-innovation-admin";
const SUPABASE_ANON_KEY = process.env.GRID_SUPABASE_ANON_KEY || "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpwaGFiZXpxYmJvYWV4bW1oY2ljIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzYxNTY5MTgsImV4cCI6MjA5MTczMjkxOH0.cHZCuzwiAEEQjPo6WAADaD5oBZapFmk45dOe4A4g37U";
const SUPABASE_SERVICE_ROLE_KEY = process.env.GRID_SUPABASE_SERVICE_ROLE_KEY || "";
const IMPORT_TOKEN = process.env.GRID_LOCAL_IMPORT_TOKEN || "";
const START_PAGE = Number(process.env.GRID_START_PAGE || 1);
const END_PAGE = Number(process.env.GRID_END_PAGE || 52);
const CHUNK_SIZE = Number(process.env.GRID_IMPORT_CHUNK_SIZE || 20);
const FORCE_AI_RECLASSIFY = process.env.GRID_FORCE_AI_RECLASSIFY === "1";
const ENABLE_AI_ENRICHMENT = process.env.GRID_ENABLE_AI_ENRICHMENT !== "0";
const REQUESTED_BY = process.env.GRID_REQUESTED_BY || process.env.GITHUB_ACTOR || "local-import";

if (!SUPABASE_SERVICE_ROLE_KEY && !IMPORT_TOKEN) {
  console.error("Missing GRID_SUPABASE_SERVICE_ROLE_KEY or GRID_LOCAL_IMPORT_TOKEN environment variable.");
  process.exit(1);
}

function cleanText(value = "") {
  return String(value).replace(/\s+/g, " ").trim();
}

function decodeHtml(value = "") {
  return cleanText(String(value)
    .replace(/&#160;|&nbsp;/gi, " ")
    .replace(/&#8211;/gi, " - ")
    .replace(/&#8217;/gi, "'")
    .replace(/&#8220;|&#8221;/gi, "\"")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, "\"")
    .replace(/&#39;/gi, "'")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/li>/gi, "\n")
    .replace(/<li[^>]*>/gi, "- ")
    .replace(/<[^>]+>/g, " "));
}

function slugify(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[^\w\s-]/g, "")
    .trim()
    .toLowerCase()
    .replace(/[-\s]+/g, "-");
}

function dedupe(values) {
  return [...new Set((values || []).map((value) => String(value || "").trim()).filter(Boolean))];
}

function dedupeLocations(values) {
  const seen = new Set();
  const output = [];
  for (const value of values || []) {
    const normalized = cleanText(String(value || "").replace(/\s*,\s*/g, ", ").replace(/\s*\|\s*/g, " | "));
    const key = normalized.toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    output.push(normalized);
  }
  return output;
}

function toUsableCoordinate(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return Math.abs(num) <= 0.0001 ? null : num;
}

function safeUrl(value = "") {
  if (!value) return "";
  return /^https?:\/\//i.test(value) ? value : new URL(value, BASE_URL).toString();
}

function hashText(value = "") {
  return createHash("sha256").update(String(value || "")).digest("hex");
}

async function fetchHtml(url) {
  const response = await fetch(url, {
    headers: {
      Accept: "text/html,application/xhtml+xml",
      "User-Agent": "GRID Innovation Directory Sync/2.0",
    },
  });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(text || `GRID page fetch failed (${response.status})`);
  }
  return await response.text();
}

function parseListingPage(html) {
  const cards = [...html.matchAll(/<div class="col-md-4">[\s\S]*?<\/div>\s*<\/div>\s*<\/div>/gi)];
  return cards.map((match) => {
    const card = match[0];
    const detailPath = card.match(/href="(\/practices\/[^"]+)"/i)?.[1] || "";
    const practiceId = detailPath.split("/").filter(Boolean).pop() || "";
    const title = decodeHtml(card.match(/<p class="card-text">\s*([\s\S]*?)\s*<\/p>/i)?.[1] || "");
    const imageUrl = safeUrl(card.match(/<img[^>]+src="([^"]+)"/i)?.[1] || "");
    const categories = [...card.matchAll(/badge-success">([^<]+)</gi)].map((item) => decodeHtml(item[1]));
    const state = decodeHtml(card.match(/badge-warning">([^<]+)</i)?.[1] || "");
    return {
      detailUrl: safeUrl(detailPath),
      practiceId,
      title,
      categories: dedupe(categories),
      state,
      imageUrl: imageUrl || null,
    };
  }).filter((item) => item.detailUrl && item.practiceId && item.title);
}

function getSection(html, heading) {
  const escaped = heading.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const regex = new RegExp(`<div class="card">[\\s\\S]*?<div class="card-header[^"]*">[\\s\\S]*?${escaped}[\\s\\S]*?<\\/div>[\\s\\S]*?<div class="card-body">([\\s\\S]*?)<\\/div>[\\s\\S]*?<\\/div>`, "i");
  return html.match(regex)?.[1] || "";
}

function parseLabeledFields(sectionHtml) {
  const fields = [];
  const chunks = [...sectionHtml.matchAll(/<b\b[^>]*>([\s\S]*?)<\/b>([\s\S]*?)(?=<b\b[^>]*>|$)/gi)];
  for (const chunk of chunks) {
    const key = decodeHtml(chunk[1]).replace(/:$/, "");
    const remainder = chunk[2].replace(/<hr[^>]*>/gi, " ");
    const value = decodeHtml(remainder.replace(/<style[\s\S]*?<\/style>/gi, ""));
    const urls = [...remainder.matchAll(/href="([^"]+)"/gi)].map((item) => safeUrl(item[1]));
    if (key && value) fields.push({ key, value, urls: dedupe(urls) });
  }
  return fields;
}

function fieldValue(fields, ...names) {
  const wanted = names.map((name) => cleanText(name).toLowerCase());
  return fields.find((field) => wanted.includes(cleanText(field.key).toLowerCase()))?.value || "";
}

function parseDetailPage(listingItem, html) {
  const title = decodeHtml(html.match(/<div class="alert alert-success"[\s\S]*?<h3[^>]*>\s*([\s\S]*?)\s*<\/h3>/i)?.[1] || listingItem.title);
  const categoryText = decodeHtml(html.match(/<div class="alert alert-danger"[\s\S]*?Category:\s*<\/b>\s*([\s\S]*?)<br>/i)?.[1] || "");
  const aboutPracticeFields = parseLabeledFields(getSection(html, "About the Practice"));
  const innovatorFields = parseLabeledFields(getSection(html, "About the Innovator"));
  const practiceFields = parseLabeledFields(getSection(html, "Practice Details"));
  const innovatorName = fieldValue(innovatorFields, "Knowledge Provider / Innovator", "Knowledge Provider", "Innovator");
  const district = fieldValue(innovatorFields, "District");
  const city = fieldValue(innovatorFields, "City");
  const state = fieldValue(innovatorFields, "State") || listingItem.state;
  const address = fieldValue(innovatorFields, "Address");
  const summary = fieldValue(aboutPracticeFields, "Detail", "Summary");
  const problemStatement = fieldValue(aboutPracticeFields, "Problem Statement");
  const location = dedupeLocations([city, district, state]).join(", ");
  const referenceText = decodeHtml(html.match(/<div class="text-muted">[\s\S]*?<small>([\s\S]*?)<\/small>/i)?.[1] || "");
  const latitude = toUsableCoordinate(html.match(/var\s+lati\s*=\s*"([^"]+)"/i)?.[1]);
  const longitude = toUsableCoordinate(html.match(/var\s+longi\s*=\s*"([^"]+)"/i)?.[1]);
  const imageUrls = dedupe([
    listingItem.imageUrl || "",
    ...[...html.matchAll(/<img[^>]+src="([^"]+\/assets\/practices\/[^"]+)"/gi)].map((item) => safeUrl(item[1])),
  ]);
  const allUrls = dedupe([
    ...aboutPracticeFields.flatMap((field) => field.urls || []),
    ...innovatorFields.flatMap((field) => field.urls || []),
    ...practiceFields.flatMap((field) => field.urls || []),
  ]);
  const videoUrls = allUrls.filter((url) => /youtube|youtu\.be|vimeo|loom|\.mp4($|\?)/i.test(url));
  const attachmentUrls = allUrls.filter((url) => !videoUrls.includes(url));
  const practiceDetails = dedupe([
    ...practiceFields.map((field) => `${field.key}: ${field.value}`),
    ...aboutPracticeFields.filter((field) => cleanText(field.key).toLowerCase() !== "detail").map((field) => `${field.key}: ${field.value}`),
  ]).join("\n");
  const innovatorDetails = dedupe(innovatorFields.map((field) => `${field.key}: ${field.value}`)).join("\n");

  return {
    ...listingItem,
    title,
    categories: dedupe([...listingItem.categories, ...categoryText.split(",").map((item) => decodeHtml(item))]),
    state,
    district,
    city,
    location,
    summary,
    problemStatement,
    innovatorName,
    innovatorDetails,
    practiceDetails,
    innovatorFields,
    practiceFields,
    aboutPracticeFields,
    referenceText,
    imageUrls,
    videoUrls,
    attachmentUrls,
    latitude,
    longitude,
    address,
  };
}

function buildVendorId(parsed) {
  return slugify([parsed.innovatorName || "innovator", parsed.district || parsed.city || parsed.state || parsed.practiceId].filter(Boolean).join("-"));
}

function buildRows(parsed) {
  const vendorId = buildVendorId(parsed);
  const productSpecifications = [
    ...parsed.aboutPracticeFields.map((field) => ({ key: field.key, value: field.value })),
    ...parsed.practiceFields.map((field) => ({ key: field.key, value: field.value })),
  ];
  const practiceTags = dedupe([
    ...parsed.categories,
    parsed.state,
    parsed.district,
    parsed.city,
    ...parsed.practiceFields.map((field) => field.key),
  ]);

  const productRow = {
    portal_product_id: parsed.practiceId,
    portal_vendor_id: vendorId,
    vendor_name: parsed.innovatorName || "Unknown Innovator",
    product_name: parsed.title,
    product_description: parsed.summary || parsed.problemStatement || null,
    product_link: parsed.detailUrl,
    product_image_url: parsed.imageUrls[0] || null,
    product_gallery_urls: parsed.imageUrls,
    product_video_urls: parsed.videoUrls,
    product_attachment_urls: parsed.attachmentUrls,
    product_location_text: parsed.location || parsed.state || null,
    product_categories: parsed.categories,
    product_subcategories: [],
    product_specifications: productSpecifications,
    practice_summary: parsed.summary || null,
    innovator_details: parsed.innovatorDetails || null,
    practice_details: parsed.practiceDetails || null,
    source_reference: parsed.referenceText || null,
    tags: practiceTags,
    search_text: dedupe([
      parsed.title,
      parsed.summary,
      parsed.problemStatement,
      parsed.innovatorName,
      parsed.innovatorDetails,
      parsed.practiceDetails,
      parsed.location,
      parsed.state,
      parsed.referenceText,
      ...practiceTags,
      ...productSpecifications.flatMap((spec) => [spec.key, spec.value]),
      ...parsed.attachmentUrls,
      ...parsed.videoUrls,
    ]).join(" "),
    raw_product: {
      about_practice_fields: parsed.aboutPracticeFields,
      innovator_fields: parsed.innovatorFields,
      practice_fields: parsed.practiceFields,
    },
    synced_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };

  const vendorRow = {
    portal_vendor_id: vendorId,
    vendor_name: parsed.innovatorName || "Unknown Innovator",
    about_vendor: parsed.innovatorDetails || null,
    website_details: parsed.detailUrl,
    location_text: dedupeLocations([parsed.address, parsed.location, parsed.state]).join(" | ") || null,
    city: parsed.city || null,
    state: parsed.state || null,
    country: "India",
    district: parsed.district || null,
    pin_code: fieldValue(parsed.innovatorFields, "PIN Code") || null,
    agro_ecological_zone: fieldValue(parsed.innovatorFields, "Agro-Ecological Zone") || null,
    service_locations: dedupeLocations([parsed.location, parsed.state]),
    tags: dedupe([...parsed.categories, parsed.state, parsed.district, ...practiceTags]),
    portal_vendor_link: parsed.detailUrl,
    portal_contact_name: parsed.innovatorName || "Unknown Innovator",
    website_address: parsed.address || null,
    contact_source_url: parsed.detailUrl,
    website_status: "GRID local import",
    legacy_products_links: parsed.detailUrl,
    contact_notes: "Imported by local GRID scraper to avoid edge compute limits.",
    innovator_image_urls: parsed.imageUrls,
    innovator_media_urls: dedupe([...parsed.videoUrls, ...parsed.attachmentUrls]),
    latitude: parsed.latitude,
    longitude: parsed.longitude,
    products_count: 1,
    search_text: dedupe([
      parsed.innovatorName,
      parsed.innovatorDetails,
      parsed.address,
      parsed.location,
      parsed.state,
      parsed.district,
      fieldValue(parsed.innovatorFields, "Agro-Ecological Zone"),
      parsed.title,
      parsed.summary,
      ...practiceTags,
    ]).join(" "),
    raw_vendor: {
      innovator_fields: parsed.innovatorFields,
      source_reference: parsed.referenceText,
    },
    synced_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };

  return { vendorRow, productRow };
}

function buildAiSourceHash(parsed, productRow) {
  return hashText(JSON.stringify({
    practiceId: parsed.practiceId,
    title: parsed.title,
    innovatorName: parsed.innovatorName,
    summary: parsed.summary,
    problemStatement: parsed.problemStatement,
    location: parsed.location,
    categories: parsed.categories,
    practiceDetails: parsed.practiceDetails,
    innovatorDetails: parsed.innovatorDetails,
    sourceReference: parsed.referenceText,
    rawProduct: productRow.raw_product,
  }));
}

async function fetchAllRows(table, select, orderColumn) {
  const rows = [];
  let from = 0;
  const pageSize = 1000;
  while (true) {
    const url = `${SUPABASE_URL}/rest/v1/${table}?select=${encodeURIComponent(select)}&order=${encodeURIComponent(orderColumn)}&offset=${from}&limit=${pageSize}`;
    const response = await fetch(url, {
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      },
    });
    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(`${table} preload failed: ${text || response.status}`);
    }
    const batch = await response.json();
    rows.push(...(Array.isArray(batch) ? batch : []));
    if (!Array.isArray(batch) || batch.length < pageSize) break;
    from += pageSize;
  }
  return rows;
}

async function loadExistingAiCache() {
  if (!SUPABASE_SERVICE_ROLE_KEY) return new Map();
  const rows = await fetchAllRows(
    "grid_practices",
    "portal_product_id,ai_model,ai_summary,ai_classified_at,ai_source_hash",
    "product_name.asc"
  );
  return new Map(rows.map((row) => [row.portal_product_id, row]));
}

async function classifyRows(rows, existingAiCache) {
  const aiEnabled = ENABLE_AI_ENRICHMENT && hasAiProviderConfigured();
  if (ENABLE_AI_ENRICHMENT && !hasAiProviderConfigured()) {
    console.warn("GRID AI enrichment is enabled but no AI provider keys were found. Continuing without AI classification.");
  }

  for (const item of rows) {
    const aiSourceHash = buildAiSourceHash(item.parsed, item.productRow);
    const existing = existingAiCache.get(item.productRow.portal_product_id);
    item.productRow.ai_source_hash = aiSourceHash;

    if (!aiEnabled) continue;
    if (!FORCE_AI_RECLASSIFY && existing?.ai_source_hash && existing.ai_source_hash === aiSourceHash && existing.ai_summary) {
      item.productRow.ai_model = existing.ai_model || null;
      item.productRow.ai_summary = existing.ai_summary || {};
      item.productRow.ai_classified_at = existing.ai_classified_at || null;
      continue;
    }

    try {
      console.log(`AI classify: ${item.productRow.product_name}`);
      const { aiModel, aiSummary } = await enrichGridPractice(buildGridAiContext(item.productRow));
      item.productRow.ai_model = aiModel;
      item.productRow.ai_summary = aiSummary;
      item.productRow.ai_classified_at = new Date().toISOString();
      item.productRow.search_text = dedupe([
        item.productRow.search_text,
        aiSummary.summary_of_practice,
        ...(aiSummary.tags || []),
        ...(aiSummary.six_m_categories || []),
        ...(aiSummary.process_steps || []),
      ]).join(" ");
    } catch (error) {
      console.warn(`AI classification failed for ${item.productRow.portal_product_id}: ${error.message}`);
      if (existing?.ai_summary) {
        item.productRow.ai_model = existing.ai_model || null;
        item.productRow.ai_summary = existing.ai_summary || {};
        item.productRow.ai_classified_at = existing.ai_classified_at || null;
      }
    }
  }
}

async function upsertRows(table, onConflict, rows) {
  if (!rows.length) return;
  const url = `${SUPABASE_URL}/rest/v1/${table}?on_conflict=${encodeURIComponent(onConflict)}`;
  for (let attempt = 1; attempt <= 4; attempt += 1) {
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          Prefer: "resolution=merge-duplicates,return=minimal",
        },
        body: JSON.stringify(rows),
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`${table} upsert failed: ${text || response.status}`);
      }
      return;
    } catch (error) {
      if (attempt === 4) throw error;
      const waitMs = attempt * 2000;
      console.warn(`${table} upsert retry ${attempt} after error: ${error.message}`);
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }
}

async function createSyncRun() {
  if (!SUPABASE_SERVICE_ROLE_KEY) return null;
  const response = await fetch(`${SUPABASE_URL}/rest/v1/grid_sync_runs`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      Prefer: "return=representation",
    },
    body: JSON.stringify([{
      status: "running",
      requested_by: REQUESTED_BY,
      started_at: new Date().toISOString(),
    }]),
  });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(text || `grid_sync_runs create failed (${response.status})`);
  }
  const data = await response.json();
  return Array.isArray(data) && data[0]?.id ? String(data[0].id) : null;
}

async function updateSyncRun(runId, updates) {
  if (!SUPABASE_SERVICE_ROLE_KEY || !runId) return;
  const response = await fetch(`${SUPABASE_URL}/rest/v1/grid_sync_runs?id=eq.${encodeURIComponent(runId)}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      Prefer: "return=minimal",
    },
    body: JSON.stringify({
      ...updates,
      updated_at: new Date().toISOString(),
    }),
  });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(text || `grid_sync_runs update failed (${response.status})`);
  }
}

async function postBatch(vendors, products, pageStart, pageEnd) {
  if (SUPABASE_SERVICE_ROLE_KEY) {
    await upsertRows("grid_innovators", "portal_vendor_id", vendors);
    await upsertRows("grid_practices", "portal_product_id", products);
    return { ok: true, vendorCount: vendors.length, productCount: products.length };
  }
  const response = await fetch(FUNCTION_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      apikey: SUPABASE_ANON_KEY,
      Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
    },
    body: JSON.stringify({
      action: "importGridBatch",
      importToken: IMPORT_TOKEN,
      requestedBy: `local-import pages ${pageStart}-${pageEnd}`,
      vendors,
      products,
    }),
  });
  const text = await response.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {}
  if (!response.ok) {
    throw new Error(data?.error || text || `Import batch failed with status ${response.status}`);
  }
  return data;
}

async function main() {
  const runId = await createSyncRun();
  const existingAiCache = await loadExistingAiCache();
  try {
    const listings = [];
    for (let page = START_PAGE; page <= END_PAGE; page += 1) {
      console.log(`Listing page ${page}/${END_PAGE}`);
      const html = await fetchHtml(page === 1 ? `${BASE_URL}/practices` : `${BASE_URL}/practices?page=${page}`);
      listings.push(...parseListingPage(html));
    }

    const uniqueListings = dedupe(listings.map((item) => item.detailUrl)).map((url) => listings.find((item) => item.detailUrl === url));
    console.log(`Found ${uniqueListings.length} listing entries`);

    const parsed = [];
    for (let index = 0; index < uniqueListings.length; index += 1) {
      const item = uniqueListings[index];
      try {
        console.log(`Detail ${index + 1}/${uniqueListings.length}: ${item.title}`);
        const html = await fetchHtml(item.detailUrl);
        parsed.push(parseDetailPage(item, html));
      } catch (error) {
        console.warn(`Skipping ${item.detailUrl}: ${error.message}`);
      }
    }

    const rows = parsed.map((item) => {
      const built = buildRows(item);
      return { parsed: item, ...built };
    });
    await classifyRows(rows, existingAiCache);
    let importedVendorCount = 0;
    let importedProductCount = 0;
    for (let index = 0; index < rows.length; index += CHUNK_SIZE) {
      const chunk = rows.slice(index, index + CHUNK_SIZE);
      const vendorMap = new Map();
      for (const item of chunk) {
        vendorMap.set(item.vendorRow.portal_vendor_id, item.vendorRow);
      }
      const vendors = [...vendorMap.values()];
      const products = chunk.map((item) => item.productRow);
      const start = index + 1;
      const end = index + chunk.length;
      console.log(`Importing chunk ${start}-${end} of ${rows.length}`);
      const result = await postBatch(vendors, products, start, end);
      importedVendorCount += Number(result.vendorCount || vendors.length || 0);
      importedProductCount += Number(result.productCount || products.length || 0);
      console.log(`Imported ${result.vendorCount} innovators and ${result.productCount} practices`);
    }

    await updateSyncRun(runId, {
      status: "success",
      finished_at: new Date().toISOString(),
      vendor_count: importedVendorCount,
      product_count: importedProductCount,
      error_message: null,
    });

    console.log(`Done. Parsed ${parsed.length} practices.`);
  } catch (error) {
    await updateSyncRun(runId, {
      status: "failed",
      finished_at: new Date().toISOString(),
      error_message: error instanceof Error ? error.message : String(error),
    }).catch(() => null);
    throw error;
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
