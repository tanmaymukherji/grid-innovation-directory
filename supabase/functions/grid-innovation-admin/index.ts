import { createClient } from "npm:@supabase/supabase-js@2";
import { load } from "npm:cheerio@1.0.0";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? Deno.env.get("SELCO_VENDOR_SERVICE_ROLE_KEY") ?? "";
const gridLocalImportToken = Deno.env.get("GRID_LOCAL_IMPORT_TOKEN") ?? "";
// The public GRID site is commonly linked under grid.undp.org.in, but the live TLS
// certificate is issued for grid.gian.org.in. The edge function must fetch via the
// certificate-matching host to avoid strict TLS failures inside Supabase.
const gridBaseUrl = "https://grid.gian.org.in";
const gridListingUrl = `${gridBaseUrl}/practices`;
const MAX_PRACTICES_PER_RUN = 40;
const DETAIL_CONCURRENCY = 6;
const STALE_RUN_MINUTES = 10;
const EDITABLE_VENDOR_FIELDS = [
  "vendor_name",
  "portal_contact_name",
  "location_text",
  "district",
  "state",
  "pin_code",
  "agro_ecological_zone",
  "final_contact_email",
  "final_contact_phone",
  "final_contact_address",
  "about_vendor",
  "website_details",
  "contact_source_url",
  "website_status",
  "contact_notes",
] as const;

type ListingItem = {
  detailUrl: string;
  practiceId: string;
  title: string;
  categories: string[];
  state: string;
  imageUrl: string | null;
};

type ParsedField = {
  key: string;
  value: string;
  urls: string[];
};

type ParsedPractice = {
  detailUrl: string;
  practiceId: string;
  title: string;
  categories: string[];
  state: string;
  district: string;
  city: string;
  location: string;
  summary: string;
  problemStatement: string;
  innovatorName: string;
  innovatorDetails: string;
  practiceDetails: string;
  innovatorFields: ParsedField[];
  practiceFields: ParsedField[];
  aboutPracticeFields: ParsedField[];
  referenceText: string;
  imageUrls: string[];
  videoUrls: string[];
  attachmentUrls: string[];
  latitude: number | null;
  longitude: number | null;
  rawHtml: string;
};

let supabaseClient: ReturnType<typeof createClient> | null = null;

function getSupabaseAdmin() {
  if (!supabaseUrl || !serviceRoleKey) throw new Error("Function secrets are not configured.");
  if (supabaseClient) return supabaseClient;
  supabaseClient = createClient(supabaseUrl, serviceRoleKey, {
    auth: { persistSession: false },
  });
  return supabaseClient;
}

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

function errorResponse(message: string, status = 400) {
  return jsonResponse({ error: message }, status);
}

function requireString(value: unknown) {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeText(value: unknown) {
  return requireString(value).toLowerCase();
}

function safeUrl(value: string) {
  if (!value) return "";
  return /^https?:\/\//i.test(value) ? value : new URL(value, gridBaseUrl).toString();
}

function dedupe(values: string[]) {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))];
}

function cleanText(value: unknown) {
  return requireString(value).replace(/\s+/g, " ").trim();
}

function decodeHtml(value: unknown) {
  return cleanText(
    requireString(value)
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
      .replace(/<[^>]+>/g, " ")
  );
}

function uniqueBy<T>(items: T[], getKey: (item: T) => string) {
  const seen = new Set<string>();
  const output: T[] = [];
  for (const item of items) {
    const key = getKey(item);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    output.push(item);
  }
  return output;
}

function normalizeLocationValue(value: unknown) {
  return requireString(value)
    .replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ")
    .replace(/\s*\|\s*/g, " | ")
    .trim();
}

function dedupeLocations(values: unknown[]) {
  const seen = new Set<string>();
  const output: string[] = [];
  for (const value of values) {
    const normalized = normalizeLocationValue(value);
    const key = normalized.toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    output.push(normalized);
  }
  return output;
}

function slugify(value: string) {
  return value
    .normalize("NFKD")
    .replace(/[^\w\s-]/g, "")
    .trim()
    .toLowerCase()
    .replace(/[-\s]+/g, "-");
}

function toNullableNumber(value: unknown) {
  const num = typeof value === "number" ? value : Number(value);
  return Number.isFinite(num) ? num : null;
}

function toUsableCoordinate(value: unknown) {
  const num = toNullableNumber(value);
  if (num === null) return null;
  return Math.abs(num) <= 0.0001 ? null : num;
}

function extractUrlsFromHtml(html: string) {
  const $ = load(`<div id="root">${html}</div>`);
  return dedupe(
    $("#root a")
      .map((_, el) => safeUrl($(el).attr("href") || ""))
      .get()
      .filter(Boolean),
  );
}

function extractTextFromHtml(html: string) {
  return decodeHtml(html.replace(/<style[\s\S]*?<\/style>/gi, " "));
}

function isVideoUrl(url: string) {
  return /youtube|youtu\.be|vimeo|loom|\.mp4($|\?)/i.test(url);
}

function isImageUrl(url: string) {
  return /\.(png|jpe?g|gif|webp|svg)($|\?)/i.test(url) || /\/assets\/practices\//i.test(url);
}

function isAttachmentUrl(url: string) {
  return /\.(pdf|docx?|xlsx?|pptx?|zip|rar|txt)($|\?)/i.test(url) || (!isVideoUrl(url) && !isImageUrl(url) && /^https?:\/\//i.test(url));
}

function parseLabeledFields(sectionHtml: string) {
  const $ = load(`<div id="root">${sectionHtml}</div>`);
  const root = $("#root");
  const fields: ParsedField[] = [];
  let currentKey = "";
  let buffer: string[] = [];

  const flush = () => {
    const key = cleanText(currentKey).replace(/:$/, "");
    if (!key) {
      buffer = [];
      currentKey = "";
      return;
    }
    const rawValue = buffer.join(" ");
    const value = extractTextFromHtml(rawValue);
    fields.push({
      key,
      value,
      urls: extractUrlsFromHtml(rawValue),
    });
    buffer = [];
    currentKey = "";
  };

  root.contents().each((_, node) => {
    if (node.type === "tag" && node.name === "b") {
      flush();
      currentKey = $(node).text();
      return;
    }
    if (node.type === "tag" && node.name === "hr") {
      flush();
      return;
    }
    if (node.type === "tag" && node.name === "style") return;
    if (currentKey) buffer.push($.html(node));
  });
  flush();
  return fields.filter((field) => field.key && field.value);
}

function fieldValue(fields: ParsedField[], ...names: string[]) {
  const wanted = names.map((name) => normalizeText(name));
  return fields.find((field) => wanted.includes(normalizeText(field.key)))?.value || "";
}

function sectionByHeading(html: string, heading: string) {
  const $ = load(html);
  const card = $(".card").filter((_, el) => normalizeText($(el).find(".card-header").text()).includes(normalizeText(heading))).first();
  return card.find(".card-body").html() || "";
}

async function fetchText(url: string) {
  const response = await fetch(url, {
    headers: {
      Accept: "text/html,application/xhtml+xml",
      "User-Agent": "GRID Directory Sync/1.0",
    },
  });
  if (!response.ok) throw new Error(`Fetch failed for ${url}: ${response.status}`);
  return await response.text();
}

function extractPageCount(html: string) {
  const lastMatch = html.match(/href="\/practices\?page=(\d+)">Last/i);
  if (lastMatch) return Number(lastMatch[1]);
  const $ = load(html);
  const numbers = $("a.page-link")
    .map((_, el) => Number.parseInt(cleanText($(el).text()), 10))
    .get()
    .filter((value) => Number.isFinite(value));
  return numbers.length ? Math.max(...numbers) : 1;
}

function parseListingPage(html: string) {
  const $ = load(html);
  const items = uniqueBy(
    $(".card.mb-4").map((_, el) => {
      const card = $(el);
      const anchor = card.find('a[href^="/practices/"]').first();
      const detailPath = anchor.attr("href") || "";
      const detailUrl = safeUrl(detailPath);
      const practiceId = detailPath.split("/").filter(Boolean).pop() || "";
      const title = cleanText(card.find(".card-text").first().text());
      const imageUrl = safeUrl(card.find("img").first().attr("src") || "") || null;
      const badges = card.find(".badge-success").map((__, badge) => cleanText($(badge).text())).get().filter(Boolean);
      const state = cleanText(card.find(".badge-warning").first().text());
      return {
        detailUrl,
        practiceId,
        title,
        categories: dedupe(badges),
        state,
        imageUrl,
      } satisfies ListingItem;
    }).get().filter((item) => item.detailUrl && item.practiceId && item.title),
    (item) => item.detailUrl,
  );
  return { items, pageCount: extractPageCount(html) };
}

async function scrapeListingPage(pageNumber: number) {
  const url = pageNumber === 1 ? gridListingUrl : `${gridListingUrl}?page=${pageNumber}`;
  const html = await fetchText(url);
  return parseListingPage(html);
}

async function scrapeAllListings() {
  const firstPage = await scrapeListingPage(1);
  const items = [...firstPage.items];
  for (let page = 2; page <= Math.max(firstPage.pageCount, 1); page += 1) {
    const result = await scrapeListingPage(page);
    items.push(...result.items);
  }
  return uniqueBy(items, (item) => item.detailUrl);
}

function parseDetailPage(listingItem: ListingItem, html: string) {
  const $ = load(html);
  const title = cleanText($(".alert-success h3").first().text()) || listingItem.title;
  const categoryText = cleanText($(".alert-danger").first().text()).replace(/^Category:\s*/i, "");
  const categories = dedupe([
    ...listingItem.categories,
    ...categoryText.split(",").map((item) => cleanText(item)),
  ]);
  const aboutPracticeHtml = sectionByHeading(html, "About the Practice");
  const aboutInnovatorHtml = sectionByHeading(html, "About the Innovator");
  const practiceDetailsHtml = sectionByHeading(html, "Practice Details");

  const aboutPracticeFields = parseLabeledFields(aboutPracticeHtml);
  const innovatorFields = parseLabeledFields(aboutInnovatorHtml);
  const practiceFields = parseLabeledFields(practiceDetailsHtml);

  const innovatorName = fieldValue(innovatorFields, "Knowledge Provider / Innovator", "Knowledge Provider", "Innovator");
  const district = fieldValue(innovatorFields, "District");
  const city = fieldValue(innovatorFields, "City");
  const state = fieldValue(innovatorFields, "State") || listingItem.state;
  const address = fieldValue(innovatorFields, "Address");
  const summary = fieldValue(aboutPracticeFields, "Detail", "Summary");
  const problemStatement = fieldValue(aboutPracticeFields, "Problem Statement");
  const location = dedupeLocations([city, district, state]).join(", ");
  const referenceText = cleanText($(".text-muted small").text());
  const latitude = toUsableCoordinate(html.match(/var\s+lati\s*=\s*"([^"]+)"/i)?.[1]);
  const longitude = toUsableCoordinate(html.match(/var\s+longi\s*=\s*"([^"]+)"/i)?.[1]);
  const imageUrls = dedupe([
    listingItem.imageUrl || "",
    ...$(".container img.img-thumbnail, .container img.img-detail, .container img.img-fluid")
      .map((_, img) => safeUrl($(img).attr("src") || ""))
      .get()
      .filter((url) => /\/assets\/practices\//i.test(url)),
  ]);
  const allUrls = dedupe([
    ...extractUrlsFromHtml(aboutPracticeHtml),
    ...extractUrlsFromHtml(aboutInnovatorHtml),
    ...extractUrlsFromHtml(practiceDetailsHtml),
  ]);
  const videoUrls = allUrls.filter(isVideoUrl);
  const attachmentUrls = allUrls.filter((url) => isAttachmentUrl(url) && !isVideoUrl(url));
  const practiceDetails = dedupe([
    ...practiceFields.map((field) => `${field.key}: ${field.value}`),
    ...aboutPracticeFields.filter((field) => normalizeText(field.key) !== "detail").map((field) => `${field.key}: ${field.value}`),
  ]).join("\n");
  const innovatorDetails = dedupe(innovatorFields.map((field) => `${field.key}: ${field.value}`)).join("\n");

  return {
    detailUrl: listingItem.detailUrl,
    practiceId: listingItem.practiceId,
    title,
    categories,
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
    rawHtml: html,
  } satisfies ParsedPractice;
}

async function mapLimit<T, R>(items: T[], batchSize: number, worker: (item: T) => Promise<R>) {
  const results: R[] = [];
  for (let index = 0; index < items.length; index += batchSize) {
    const batch = items.slice(index, index + batchSize);
    const batchResults = await Promise.all(batch.map((item) => worker(item)));
    results.push(...batchResults);
  }
  return results;
}

async function upsertInBatches(table: string, rows: Record<string, unknown>[], onConflict: string, batchSize: number) {
  const supabase = getSupabaseAdmin();
  for (let index = 0; index < rows.length; index += batchSize) {
    const batch = rows.slice(index, index + batchSize);
    if (!batch.length) continue;
    const { error } = await supabase.from(table).upsert(batch, { onConflict });
    if (error) throw new Error(`${table} upsert failed: ${error.message}`);
  }
}

async function hashToken(token: string) {
  const bytes = new TextEncoder().encode(token);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest)).map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function generateToken() {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return Array.from(bytes).map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function validateSession(token: string) {
  const supabase = getSupabaseAdmin();
  const tokenHash = await hashToken(token);
  const { data, error } = await supabase.from("grameee_admin_sessions").select("id, username, expires_at").eq("token_hash", tokenHash).maybeSingle();
  if (error || !data) return null;
  if (new Date(data.expires_at).getTime() <= Date.now()) {
    await supabase.from("grameee_admin_sessions").delete().eq("id", data.id);
    return null;
  }
  await supabase.from("grameee_admin_sessions").update({ last_used_at: new Date().toISOString() }).eq("id", data.id);
  return data;
}

async function verifyAdminPassword(username: string, password: string) {
  const supabase = getSupabaseAdmin();
  const { data, error } = await supabase.rpc("grameee_admin_password_matches", { p_username: username, p_password: password });
  if (error) throw new Error(`Admin password verification failed: ${error.message}`);
  return Boolean(data);
}

async function handleLogin(password: string) {
  const supabase = getSupabaseAdmin();
  const { data, error } = await supabase.from("grameee_admin_accounts").select("username, password_hash").eq("username", "admin").maybeSingle();
  if (error) return errorResponse(`Admin account lookup failed: ${error.message}`, 500);
  if (!data?.password_hash) return errorResponse("Admin account does not exist yet.", 401);
  const validPassword = await verifyAdminPassword("admin", password).catch(() => false);
  if (!validPassword) return errorResponse("Invalid admin password.", 401);

  const token = generateToken();
  const tokenHash = await hashToken(token);
  const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();
  await supabase.from("grameee_admin_sessions").delete().eq("username", "admin");
  const { error: sessionError } = await supabase.from("grameee_admin_sessions").insert({ username: "admin", token_hash: tokenHash, expires_at: expiresAt });
  if (sessionError) return errorResponse("Admin session could not be created.", 500);
  return jsonResponse({ token, username: "admin", expires_at: expiresAt });
}

async function handleVerify(token: string) {
  const session = await validateSession(token);
  return jsonResponse({ valid: Boolean(session), username: session?.username ?? null, expires_at: session?.expires_at ?? null });
}

async function handleLogout(token: string) {
  const supabase = getSupabaseAdmin();
  const tokenHash = await hashToken(token);
  await supabase.from("grameee_admin_sessions").delete().eq("token_hash", tokenHash);
  return jsonResponse({ ok: true });
}

async function getSyncState() {
  const supabase = getSupabaseAdmin();
  const { data, error } = await supabase.from("grid_sync_state").select("*").eq("state_key", "default").maybeSingle();
  if (error) throw new Error(`Could not load GRID sync state: ${error.message}`);
  if (data) return data;
  const { data: inserted, error: insertError } = await supabase
    .from("grid_sync_state")
    .insert({ state_key: "default", next_offset: 0, last_total: 0 })
    .select("*")
    .single();
  if (insertError || !inserted) throw new Error(`Could not initialize GRID sync state: ${insertError?.message || "unknown error"}`);
  return inserted;
}

async function updateSyncState(values: Record<string, unknown>) {
  const supabase = getSupabaseAdmin();
  const { error } = await supabase.from("grid_sync_state").upsert({
    state_key: "default",
    updated_at: new Date().toISOString(),
    ...values,
  }, { onConflict: "state_key" });
  if (error) throw new Error(`Could not update GRID sync state: ${error.message}`);
}

function sliceBatch<T>(items: T[], offset: number, batchSize: number) {
  if (!items.length) return [];
  const normalizedOffset = ((offset % items.length) + items.length) % items.length;
  const primary = items.slice(normalizedOffset, normalizedOffset + batchSize);
  if (primary.length >= batchSize || primary.length === items.length) return primary;
  return [...primary, ...items.slice(0, Math.min(batchSize - primary.length, normalizedOffset))];
}

function buildVendorId(parsed: ParsedPractice) {
  return slugify([parsed.innovatorName || "innovator", parsed.district || parsed.city || parsed.state || parsed.practiceId].filter(Boolean).join("-"));
}

async function markStaleRunningSyncs() {
  const supabase = getSupabaseAdmin();
  const staleBefore = new Date(Date.now() - STALE_RUN_MINUTES * 60 * 1000).toISOString();
  const { error } = await supabase
    .from("grid_sync_runs")
    .update({
      status: "failed",
      finished_at: new Date().toISOString(),
      error_message: `Marked failed automatically after exceeding ${STALE_RUN_MINUTES} minutes in running state.`,
      updated_at: new Date().toISOString(),
    })
    .eq("status", "running")
    .lt("started_at", staleBefore);
  if (error) throw new Error(`Could not update stale GRID sync runs: ${error.message}`);
}

async function handleListGridSyncRuns(token: string) {
  const supabase = getSupabaseAdmin();
  const session = await validateSession(token);
  if (!session) return errorResponse("Invalid admin session.", 401);
  await markStaleRunningSyncs();
  const { data, error } = await supabase.from("grid_sync_runs").select("*").order("created_at", { ascending: false }).limit(10);
  if (error) return errorResponse("GRID sync runs could not be loaded.", 500);
  return jsonResponse({ items: data ?? [] });
}

async function handleUpdateGridInnovator(token: string, portalVendorId: string, updates: Record<string, unknown>) {
  const supabase = getSupabaseAdmin();
  const session = await validateSession(token);
  if (!session) return errorResponse("Invalid admin session.", 401);
  if (!portalVendorId) return errorResponse("Missing innovator id.", 400);

  const cleanUpdates: Record<string, unknown> = {};
  for (const field of EDITABLE_VENDOR_FIELDS) {
    if (!(field in updates)) continue;
    const value = requireString(updates[field]);
    cleanUpdates[field] = value || null;
  }
  if (!Object.keys(cleanUpdates).length) return errorResponse("No valid fields were provided for update.", 400);
  cleanUpdates.updated_at = new Date().toISOString();

  const { data, error } = await supabase
    .from("grid_innovators")
    .update(cleanUpdates)
    .eq("portal_vendor_id", portalVendorId)
    .select("*")
    .single();
  if (error) return errorResponse(`Innovator update failed: ${error.message}`, 500);
  return jsonResponse({ ok: true, item: data });
}

async function runGridSync(requestedBy: string) {
  const supabase = getSupabaseAdmin();
  await markStaleRunningSyncs();
  const syncState = await getSyncState();
  const { data: runData, error: runError } = await supabase.from("grid_sync_runs").insert({
    status: "running",
    requested_by: requestedBy,
    started_at: new Date().toISOString(),
  }).select("id").single();
  if (runError || !runData?.id) throw new Error("GRID sync run could not be created.");
  const runId = String(runData.id);

  try {
    const listingItems = await scrapeAllListings();
    const selectedListings = sliceBatch(listingItems, Number(syncState.next_offset || 0), MAX_PRACTICES_PER_RUN);
    await updateSyncState({
      last_started_at: new Date().toISOString(),
      last_total: listingItems.length,
    });

    const parsedPractices = (await mapLimit(selectedListings, DETAIL_CONCURRENCY, async (listingItem) => {
      try {
        const html = await fetchText(listingItem.detailUrl);
        return parseDetailPage(listingItem, html);
      } catch {
        return null;
      }
    })).filter(Boolean) as ParsedPractice[];

    const productRows = parsedPractices.map((parsed) => {
      const vendorId = buildVendorId(parsed);
      const practiceTags = dedupe([
        ...parsed.categories,
        parsed.state,
        parsed.district,
        parsed.city,
        ...parsed.practiceFields.map((field) => field.key),
      ]);
      const productSpecifications = [
        ...parsed.aboutPracticeFields.map((field) => ({ key: field.key, value: field.value })),
        ...parsed.practiceFields.map((field) => ({ key: field.key, value: field.value })),
      ];
      return {
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
          raw_html: parsed.rawHtml,
        },
        synced_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };
    });

    const vendorRows = uniqueBy(parsedPractices.map((parsed) => {
      const vendorId = buildVendorId(parsed);
      const linkedProducts = productRows.filter((product) => product.portal_vendor_id === vendorId);
      const address = fieldValue(parsed.innovatorFields, "Address");
      const locationText = dedupeLocations([address, parsed.location, parsed.state]).join(" | ");
      return {
        portal_vendor_id: vendorId,
        vendor_name: parsed.innovatorName || "Unknown Innovator",
        about_vendor: parsed.innovatorDetails || null,
        website_details: parsed.detailUrl,
        location_text: locationText || null,
        city: parsed.city || null,
        state: parsed.state || null,
        country: "India",
        district: parsed.district || null,
        pin_code: fieldValue(parsed.innovatorFields, "PIN Code") || null,
        agro_ecological_zone: fieldValue(parsed.innovatorFields, "Agro-Ecological Zone") || null,
        service_locations: dedupeLocations([parsed.location, parsed.state]),
        tags: dedupe([...parsed.categories, parsed.state, parsed.district, ...linkedProducts.flatMap((item) => item.tags || [])]),
        portal_vendor_link: parsed.detailUrl,
        portal_contact_name: parsed.innovatorName || "Unknown Innovator",
        website_address: address || null,
        contact_source_url: parsed.detailUrl,
        website_status: "GRID detail page",
        legacy_products_links: linkedProducts.map((item) => item.product_link).filter(Boolean).join("\n"),
        contact_notes: "Innovator details and address were captured directly from the GRID detail page.",
        innovator_image_urls: parsed.imageUrls,
        innovator_media_urls: dedupe([...parsed.videoUrls, ...parsed.attachmentUrls]),
        latitude: parsed.latitude,
        longitude: parsed.longitude,
        products_count: linkedProducts.length,
        search_text: dedupe([
          parsed.innovatorName,
          parsed.innovatorDetails,
          address,
          parsed.location,
          parsed.state,
          parsed.district,
          fieldValue(parsed.innovatorFields, "Agro-Ecological Zone"),
          ...linkedProducts.flatMap((item) => [item.product_name, item.product_description || "", ...(item.tags || [])]),
        ]).join(" "),
        raw_vendor: {
          innovator_fields: parsed.innovatorFields,
          source_reference: parsed.referenceText,
        },
        synced_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };
    }), (row) => requireString(row.portal_vendor_id));

    await upsertInBatches("grid_innovators", vendorRows, "portal_vendor_id", 100);
    await upsertInBatches("grid_practices", productRows, "portal_product_id", 100);

    const nextOffset = listingItems.length
      ? (Number(syncState.next_offset || 0) + selectedListings.length) % listingItems.length
      : 0;
    await updateSyncState({
      next_offset: nextOffset,
      last_total: listingItems.length,
      last_finished_at: new Date().toISOString(),
    });

    await supabase.from("grid_sync_runs").update({
      status: "success",
      finished_at: new Date().toISOString(),
      vendor_count: vendorRows.length,
      product_count: productRows.length,
      error_message: null,
      updated_at: new Date().toISOString(),
    }).eq("id", runId);

    return { vendorCount: vendorRows.length, productCount: productRows.length };
  } catch (error) {
    const message = error instanceof Error ? error.message : "GRID directory sync failed.";
    await updateSyncState({
      last_finished_at: new Date().toISOString(),
    }).catch(() => null);
    await supabase.from("grid_sync_runs").update({
      status: "failed",
      finished_at: new Date().toISOString(),
      error_message: message,
      updated_at: new Date().toISOString(),
    }).eq("id", runId);
    throw error;
  }
}

async function handleSyncGridDirectory(token: string) {
  const session = await validateSession(token);
  if (!session) return errorResponse("Invalid admin session.", 401);
  try {
    const result = await runGridSync(session.username);
    return jsonResponse({ ok: true, ...result });
  } catch (error) {
    return errorResponse(error instanceof Error ? error.message : "GRID directory sync failed.", 500);
  }
}

async function handleScheduledSync(receivedToken: string) {
  if (receivedToken) return errorResponse("Scheduled sync is disabled. Run sync manually from the admin page.", 403);
  return errorResponse("Scheduled sync is disabled. Run sync manually from the admin page.", 403);
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function sanitizeImportRows(rows: unknown) {
  if (!Array.isArray(rows)) return [];
  return rows.filter((item) => isObjectRecord(item)) as Record<string, unknown>[];
}

async function handleImportGridBatch(importToken: string, requestedBy: string, vendors: unknown, products: unknown) {
  if (!gridLocalImportToken || importToken !== gridLocalImportToken) {
    return errorResponse("Invalid local import token.", 401);
  }

  const vendorRows = sanitizeImportRows(vendors).filter((row) => requireString(row.portal_vendor_id));
  const productRows = sanitizeImportRows(products).filter((row) => requireString(row.portal_product_id) && requireString(row.portal_vendor_id));

  if (!vendorRows.length && !productRows.length) {
    return errorResponse("No valid import rows were provided.", 400);
  }

  const supabase = getSupabaseAdmin();
  const { data: runData, error: runError } = await supabase.from("grid_sync_runs").insert({
    status: "running",
    requested_by: requestedBy || "local-import",
    started_at: new Date().toISOString(),
  }).select("id").single();
  if (runError || !runData?.id) return errorResponse("GRID import run could not be created.", 500);
  const runId = String(runData.id);

  try {
    await upsertInBatches("grid_innovators", vendorRows, "portal_vendor_id", 100);
    await upsertInBatches("grid_practices", productRows, "portal_product_id", 100);
    await supabase.from("grid_sync_runs").update({
      status: "success",
      finished_at: new Date().toISOString(),
      vendor_count: vendorRows.length,
      product_count: productRows.length,
      error_message: null,
      updated_at: new Date().toISOString(),
    }).eq("id", runId);
    return jsonResponse({ ok: true, vendorCount: vendorRows.length, productCount: productRows.length });
  } catch (error) {
    const message = error instanceof Error ? error.message : "GRID import batch failed.";
    await supabase.from("grid_sync_runs").update({
      status: "failed",
      finished_at: new Date().toISOString(),
      error_message: message,
      updated_at: new Date().toISOString(),
    }).eq("id", runId);
    return errorResponse(message, 500);
  }
}

Deno.serve(async (request) => {
  if (request.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  if (request.method !== "POST") return errorResponse("Method not allowed.", 405);
  if (!supabaseUrl || !serviceRoleKey) return errorResponse("Function secrets are not configured.", 500);

  let body: Record<string, unknown>;
  try { body = await request.json(); } catch { return errorResponse("Invalid JSON body.", 400); }

  const action = requireString(body.action);
  const token = requireString(body.token);
  const password = requireString(body.password);
  const receivedCronToken = requireString(body.cronToken);
  const importToken = requireString(body.importToken);
  const requestedBy = requireString(body.requestedBy);
  const portalVendorId = requireString(body.portalVendorId);
  const updates = (body.updates && typeof body.updates === "object" && !Array.isArray(body.updates))
    ? body.updates as Record<string, unknown>
    : {};
  const vendors = body.vendors;
  const products = body.products;

  switch (action) {
    case "login":
      return await handleLogin(password);
    case "verify":
      return await handleVerify(token);
    case "logout":
      return await handleLogout(token);
    case "listGridSyncRuns":
      return await handleListGridSyncRuns(token);
    case "syncGridDirectory":
      return await handleSyncGridDirectory(token);
    case "updateGridInnovator":
      return await handleUpdateGridInnovator(token, portalVendorId, updates);
    case "importGridBatch":
      return await handleImportGridBatch(importToken, requestedBy, vendors, products);
    case "scheduledSync":
      return await handleScheduledSync(receivedCronToken);
    default:
      return errorResponse("Unknown admin action.", 400);
  }
});
