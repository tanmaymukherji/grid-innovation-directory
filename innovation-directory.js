const directoryState = {
  vendors: [],
  products: [],
  filteredVendors: [],
  currentPage: 1,
  pageSize: 12,
  hasSearched: false,
  geocodeCache: new Map(),
  map: null,
  mapReady: false,
  mapLoadPromise: null,
  markers: [],
  selectedVendorId: null,
};

const INDIA_CENTER = { lat: 22.9734, lng: 78.6569 };
const SEARCH_STATE_KEY = 'grid_innovation_search_state_v1';
const SIX_M_OPTIONS = ['Manpower', 'Method', 'Material', 'Machine', 'Money', 'Market'];
const searchEls = {
  supplier: document.getElementById('search-supplier'),
  product: document.getElementById('search-product'),
  location: document.getElementById('search-location'),
  tags: document.getElementById('search-tags'),
  sixm: document.getElementById('search-sixm'),
  keyword: document.getElementById('search-keyword'),
};

const resultsEl = document.getElementById('vendor-results');
const statusEl = document.getElementById('directory-status');
const resultsSummaryEl = document.getElementById('results-summary');
const paginationEls = [
  document.getElementById('results-pagination-top'),
  document.getElementById('results-pagination-bottom'),
];

function uniqueSortedValues(values) {
  return [...new Set(values.map((value) => String(value || '').trim()).filter(Boolean))]
    .sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }));
}

function getSelectedValues(selectEl) {
  if (!selectEl) return [];
  return Array.from(selectEl.querySelectorAll('input[type="checkbox"]:checked'))
    .map((input) => String(input.value || '').trim())
    .filter(Boolean);
}

function setSelectedValues(selectEl, values) {
  if (!selectEl) return;
  const wanted = new Set((values || []).map((value) => String(value || '').trim()).filter(Boolean));
  Array.from(selectEl.querySelectorAll('input[type="checkbox"]')).forEach((input) => {
    input.checked = wanted.has(input.value);
  });
}

function populateSelectOptions(selectEl, values, placeholder) {
  if (!selectEl) return;
  const previousValue = selectEl.value;
  selectEl.innerHTML = '';
  const defaultOption = document.createElement('option');
  defaultOption.value = '';
  defaultOption.textContent = placeholder;
  selectEl.appendChild(defaultOption);
  values.forEach((value) => {
    const option = document.createElement('option');
    option.value = value;
    option.textContent = value;
    selectEl.appendChild(option);
  });
  selectEl.value = values.includes(previousValue) ? previousValue : '';
}

function getEffectiveProductTags(product) {
  const reviewed = Array.isArray(product?.reviewed_tags) ? product.reviewed_tags.filter(Boolean) : [];
  const aiTags = Array.isArray(product?.ai_summary?.tags) ? product.ai_summary.tags.filter(Boolean) : [];
  return reviewed.length ? reviewed : (aiTags.length ? aiTags : (Array.isArray(product?.tags) ? product.tags.filter(Boolean) : []));
}

function getEffectiveProductSixM(product) {
  const reviewed = Array.isArray(product?.six_m_categories) ? product.six_m_categories.filter(Boolean) : [];
  const aiSixM = Array.isArray(product?.ai_summary?.six_m_categories) ? product.ai_summary.six_m_categories.filter(Boolean) : [];
  return reviewed.length ? reviewed : aiSixM;
}

function buildTownStateLabel(town, state) {
  const cleanTown = String(town || '').trim();
  const cleanState = String(state || '').trim();
  if (cleanTown && cleanState) return `${cleanTown}, ${cleanState}`;
  return cleanTown || cleanState || '';
}

function extractTownStateFromText(value) {
  const text = String(value || '').split('|')[0].trim();
  if (!text) return '';
  const parts = text.split(',').map((part) => part.trim()).filter(Boolean);
  if (parts.length >= 2) {
    return buildTownStateLabel(parts[0], parts[parts.length - 1]);
  }
  return text;
}

function collectLocationValues() {
  return uniqueSortedValues([
    ...directoryState.vendors.map((vendor) => buildTownStateLabel(vendor.city, vendor.state)),
    ...directoryState.products.map((product) => extractTownStateFromText(product.product_location_text)),
  ]);
}

function collectTagValues() {
  return uniqueSortedValues([
    ...directoryState.vendors.flatMap((vendor) => vendor.tags || []),
    ...directoryState.products.flatMap((product) => [
      ...getEffectiveProductTags(product),
      ...(product.product_categories || []),
      ...(product.product_subcategories || []),
    ]),
  ]);
}

function populateFilterOptions() {
  populateSelectOptions(
    searchEls.supplier,
    uniqueSortedValues(directoryState.vendors.map((vendor) => vendor.vendor_name)),
    'All innovators'
  );
  populateSelectOptions(
    searchEls.product,
    uniqueSortedValues(directoryState.products.map((product) => product.product_name)),
    'All practices'
  );
  populateSelectOptions(
    searchEls.location,
    collectLocationValues(),
    'All locations'
  );
  populateSelectOptions(
    searchEls.tags,
    collectTagValues(),
    'All tags'
  );
  const previousSixM = getSelectedValues(searchEls.sixm);
  searchEls.sixm.innerHTML = SIX_M_OPTIONS.map((value) => `
    <label class="checkbox-item">
      <input type="checkbox" value="${esc(value)}" />
      <span>${esc(value)}</span>
    </label>
  `).join('');
  setSelectedValues(searchEls.sixm, previousSixM);
}

function esc(value) {
  return String(value || '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;');
}

function persistSearchState() {
  const snapshot = {
    search: {
      supplier: searchEls.supplier.value,
      product: searchEls.product.value,
      location: searchEls.location.value,
      tags: searchEls.tags.value,
      sixm: getSelectedValues(searchEls.sixm),
      keyword: searchEls.keyword.value,
    },
    currentPage: directoryState.currentPage,
    hasSearched: directoryState.hasSearched,
    selectedVendorId: directoryState.selectedVendorId,
  };
  try {
    window.sessionStorage.setItem(SEARCH_STATE_KEY, JSON.stringify(snapshot));
  } catch {}
}

function restoreSearchState() {
  try {
    const raw = window.sessionStorage.getItem(SEARCH_STATE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch {
    return null;
  }
}

function applySearchSnapshot(snapshot) {
  if (!snapshot?.search) return;
  searchEls.supplier.value = String(snapshot.search.supplier || '');
  searchEls.product.value = String(snapshot.search.product || '');
  searchEls.location.value = String(snapshot.search.location || '');
  searchEls.tags.value = String(snapshot.search.tags || '');
  setSelectedValues(searchEls.sixm, Array.isArray(snapshot.search.sixm) ? snapshot.search.sixm : []);
  searchEls.keyword.value = String(snapshot.search.keyword || '');
  directoryState.currentPage = Number(snapshot.currentPage || 1);
  directoryState.selectedVendorId = snapshot.selectedVendorId || null;
}

function normalizeText(value) {
  return String(value || '').trim().toLowerCase();
}

function tokenize(value) {
  return normalizeText(value).split(/[^a-z0-9]+/).filter(Boolean);
}

function buildVendorIndex(vendor) {
  const practiceNames = (vendor.products || []).map((product) => normalizeText(product.product_name)).join(' ');
  const practiceDescriptions = (vendor.products || []).map((product) => normalizeText(product.product_description || product.practice_summary || product.ai_summary?.summary_of_practice || product.practice_details)).join(' ');
  const tags = [
    ...(vendor.tags || []),
    ...(vendor.products || []).flatMap((product) => getEffectiveProductTags(product)),
    ...(vendor.products || []).flatMap((product) => product.product_categories || []),
    ...(vendor.products || []).flatMap((product) => (product.product_specifications || []).flatMap((spec) => [spec?.key, spec?.value])),
  ].map(normalizeText).join(' ');
  const sixm = uniqueSortedValues((vendor.products || []).flatMap((product) => getEffectiveProductSixM(product))).map(normalizeText);
  const locations = [vendor.location_text, vendor.city, vendor.district, vendor.state, vendor.country, vendor.final_contact_address, ...(vendor.service_locations || [])].map(normalizeText).join(' ');
  const keyword = [vendor.vendor_name, vendor.about_vendor, practiceNames, practiceDescriptions, tags, locations, vendor.search_text].map(normalizeText).join(' ');
  return {
    supplier: normalizeText(vendor.vendor_name),
    products: practiceNames,
    location: locations,
    tags,
    sixm,
    keyword,
  };
}

function scoreAgainstTokens(haystack, tokens, weight) {
  if (!tokens.length) return 0;
  let score = 0;
  for (const token of tokens) {
    if (!haystack.includes(token)) return null;
    score += haystack === token ? weight * 3 : haystack.startsWith(token) ? weight * 2 : weight;
  }
  return score;
}

function tokensMatchAll(haystack, tokens) {
  return tokens.every((token) => haystack.includes(token));
}

function scoreVendor(vendor, filters) {
  const index = vendor._searchIndex || (vendor._searchIndex = buildVendorIndex(vendor));
  let score = 0;

  const supplierScore = scoreAgainstTokens(index.supplier, filters.supplierTokens, 24);
  if (supplierScore === null) return null;
  score += supplierScore;

  const productScore = scoreAgainstTokens(index.products, filters.productTokens, 22);
  if (productScore === null) return null;
  score += productScore;

  const locationScore = scoreAgainstTokens(index.location, filters.locationTokens, 14);
  if (locationScore === null) return null;
  score += locationScore;

  const tagScore = scoreAgainstTokens(index.tags, filters.tagTokens, 12);
  if (tagScore === null) return null;
  score += tagScore;

  if (filters.sixmValues.length) {
    const storySixM = new Set(index.sixm || []);
    const matchedSixMCount = filters.sixmValues.filter((value) => storySixM.has(value)).length;
    if (!matchedSixMCount) return null;
    score += matchedSixMCount * 18;
  }

  if (filters.keywordTokens.length) {
    if (!tokensMatchAll(index.keyword, filters.keywordTokens)) return null;
    score += filters.keywordTokens.reduce((total, token) => total + (index.supplier.includes(token) ? 18 : 8), 0);
  }

  if (filters.keywordPhrase && index.keyword.includes(filters.keywordPhrase)) score += 30;
  if ((vendor.products_count || vendor.products?.length || 0) > 0) score += 3;
  if (vendor.latitude && vendor.longitude) score += 5;
  return score;
}

function getFilters() {
  const supplier = normalizeText(searchEls.supplier.value);
  const product = normalizeText(searchEls.product.value);
  const location = normalizeText(searchEls.location.value);
  const tags = normalizeText(searchEls.tags.value);
  const sixm = getSelectedValues(searchEls.sixm).map(normalizeText).filter(Boolean);
  const keyword = normalizeText(searchEls.keyword.value);
  return {
    supplierPhrase: supplier,
    productPhrase: product,
    locationPhrase: location,
    tagPhrase: tags,
    sixmValues: sixm,
    keywordPhrase: keyword,
    supplierTokens: tokenize(supplier),
    productTokens: tokenize(product),
    locationTokens: tokenize(location),
    tagTokens: tokenize(tags),
    keywordTokens: tokenize(keyword),
  };
}

function hasAnyFilter(filters) {
  return Boolean(
    filters.supplierTokens.length ||
    filters.productTokens.length ||
    filters.locationTokens.length ||
    filters.tagTokens.length ||
    filters.sixmValues.length ||
    filters.keywordTokens.length
  );
}

function setCounts() {
  document.getElementById('vendor-total-count').textContent = String(directoryState.vendors.length);
  document.getElementById('product-total-count').textContent = String(directoryState.products.length);
  document.getElementById('filtered-vendor-count').textContent = String(directoryState.filteredVendors.length);
}

function getPageCount() {
  return Math.max(1, Math.ceil(directoryState.filteredVendors.length / directoryState.pageSize));
}

function getPageResults() {
  const start = (directoryState.currentPage - 1) * directoryState.pageSize;
  return directoryState.filteredVendors.slice(start, start + directoryState.pageSize);
}

function setSelectedVendor(vendorId) {
  directoryState.selectedVendorId = vendorId || null;
  document.querySelectorAll('[data-vendor-card]').forEach((card) => {
    card.classList.toggle('active', card.dataset.vendorCard === vendorId);
  });
}

function focusVendor(vendorId, options = {}) {
  if (!vendorId) return;
  setSelectedVendor(vendorId);
  persistSearchState();
  if (!options.scroll) return;
  const escapedId = window.CSS?.escape ? window.CSS.escape(vendorId) : vendorId.replace(/"/g, '\\"');
  const card = document.querySelector(`[data-vendor-card="${escapedId}"]`);
  if (card) card.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

function ensureMapCss() {
  if (document.getElementById('mappls-web-sdk-css')) return;
  const link = document.createElement('link');
  link.id = 'mappls-web-sdk-css';
  link.rel = 'stylesheet';
  link.href = 'https://apis.mappls.com/vector_map/assets/v3.5/mappls-glob.css';
  document.head.appendChild(link);
}

async function loadMapSdk() {
  const key = String(window.APP_CONFIG?.MAPMYINDIA_MAP_KEY || '').trim();
  if (!key) {
    document.getElementById('results-map').innerHTML = '<div class="vendor-map-placeholder">Add `MAPMYINDIA_MAP_KEY` in `config.js` to enable the map.</div>';
    return false;
  }
  if (window.mappls?.Map) return true;
  ensureMapCss();
  const urls = [
    `https://sdk.mappls.com/map/sdk/web?v=3.0&access_token=${encodeURIComponent(key)}`,
    `https://sdk.mappls.com/map/sdk/web?v=3.0&layer=vector&access_token=${encodeURIComponent(key)}`,
    `https://apis.mappls.com/advancedmaps/api/${encodeURIComponent(key)}/map_sdk?layer=vector&v=3.0`,
  ];
  for (const src of urls) {
    try {
      await new Promise((resolve, reject) => {
        document.querySelectorAll('script[data-mappls-sdk="true"]').forEach((node) => node.remove());
        const script = document.createElement('script');
        script.src = src;
        script.async = true;
        script.defer = true;
        script.dataset.mapplsSdk = 'true';
        script.onload = () => window.mappls?.Map ? resolve() : reject(new Error('Mappls SDK unavailable'));
        script.onerror = reject;
        document.head.appendChild(script);
      });
      return true;
    } catch {}
  }
  document.getElementById('results-map').innerHTML = '<div class="vendor-map-placeholder">The MapMyIndia SDK could not be loaded for this page.</div>';
  return false;
}

async function ensureMap() {
  if (directoryState.mapReady) return true;
  if (directoryState.mapLoadPromise) return await directoryState.mapLoadPromise;
  const loaded = await loadMapSdk();
  if (!loaded || !window.mappls?.Map) return false;
  directoryState.mapLoadPromise = new Promise((resolve) => {
    directoryState.map = new window.mappls.Map('results-map', {
      center: INDIA_CENTER,
      zoom: 4.8,
      zoomControl: true,
      geolocation: false,
      location: false,
    });
    let settled = false;
    const markReady = () => {
      if (settled) return;
      settled = true;
      directoryState.mapReady = true;
      resolve(true);
    };
    directoryState.map?.on?.('load', markReady);
    directoryState.map?.addListener?.('load', markReady);
    window.setTimeout(markReady, 1500);
  });
  return await directoryState.mapLoadPromise;
}

async function geocodeVendor(vendor) {
  const cacheKey = vendor.portal_vendor_id;
  if (directoryState.geocodeCache.has(cacheKey)) return directoryState.geocodeCache.get(cacheKey);
  const lat = Number(vendor.latitude);
  const lng = Number(vendor.longitude);
  if (Number.isFinite(lat) && Number.isFinite(lng) && (Math.abs(lat) > 0.0001 || Math.abs(lng) > 0.0001)) {
    const point = { lat, lng };
    directoryState.geocodeCache.set(cacheKey, point);
    return point;
  }
  const query = [vendor.location_text, vendor.district, vendor.state, vendor.country, vendor.final_contact_address].filter(Boolean).join(', ');
  if (!query) return null;
  try {
    const response = await fetch(`https://nominatim.openstreetmap.org/search?format=jsonv2&limit=1&q=${encodeURIComponent(query)}`, {
      headers: { Accept: 'application/json' },
    });
    const data = await response.json();
    const match = Array.isArray(data) ? data[0] : null;
    if (!match) return null;
    const point = { lat: Number(match.lat), lng: Number(match.lon) };
    directoryState.geocodeCache.set(cacheKey, point);
    return point;
  } catch {
    return null;
  }
}

function clearMapMarkers() {
  directoryState.markers.forEach((marker) => marker?.remove?.());
  directoryState.markers = [];
}

function groupMapPoints(entries) {
  const groups = new Map();
  entries.forEach((entry) => {
    const key = `${entry.point.lat.toFixed(3)}|${entry.point.lng.toFixed(3)}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(entry);
  });
  return Array.from(groups.values());
}

function buildPopupHtml(entries) {
  return `<div class="vendor-map-popup">${entries.map(({ vendor }) => `<div><strong>${esc(vendor.vendor_name)}</strong><br/>${esc(vendor.location_text || 'Location not listed')}<br/><a href="./vendor-detail.html?vendor=${encodeURIComponent(vendor.portal_vendor_id)}">View Details</a> | <a href="${esc(vendor.portal_vendor_link || '#')}" target="_blank" rel="noreferrer">Open GRID Source</a></div>`).join('<hr style="border:none;border-top:1px solid #dbe5eb;margin:.55rem 0;" />')}</div>`;
}

function createRingPoints(point, count) {
  if (count <= 1) return [point];
  const radius = Math.min(0.08, 0.012 + (count * 0.0025));
  return Array.from({ length: count }, (_, index) => {
    const angle = (Math.PI * 2 * index) / count;
    const latOffset = Math.sin(angle) * radius;
    const lngOffset = Math.cos(angle) * radius / Math.max(Math.cos((point.lat * Math.PI) / 180), 0.35);
    return {
      lat: point.lat + latOffset,
      lng: point.lng + lngOffset,
    };
  });
}

function buildMarkerHtml(count) {
  const size = count > 1 ? 34 : 20;
  const halo = count > 1 ? 10 : 7;
  const border = count > 1 ? 4 : 3;
  const label = count > 1 ? `<span style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#fff;font:700 13px/1 'Segoe UI',Arial,sans-serif;">${count}</span>` : '';
  return `<div style="position:relative;width:${size}px;height:${size}px;border-radius:999px;background:#f57c00;border:${border}px solid #fff;box-shadow:0 0 0 ${halo}px rgba(245,124,0,.18),0 8px 18px rgba(245,124,0,.28);">${label}</div>`;
}

function getBoundsZoom(spanLat, spanLng) {
  const span = Math.max(spanLat, spanLng);
  if (span < 0.08) return 9.5;
  if (span < 0.2) return 8.5;
  if (span < 0.5) return 7.5;
  if (span < 1.5) return 6.5;
  if (span < 4) return 5.5;
  return 4.8;
}

function fitMapToPoints(activePoints) {
  if (!directoryState.map) return;
  if (!activePoints.length) {
    directoryState.map?.setCenter?.(INDIA_CENTER);
    directoryState.map?.setZoom?.(4.8);
    return;
  }
  const lats = activePoints.map(({ point }) => Number(point.lat));
  const lngs = activePoints.map(({ point }) => Number(point.lng));
  const minLat = Math.min(...lats);
  const maxLat = Math.max(...lats);
  const minLng = Math.min(...lngs);
  const maxLng = Math.max(...lngs);
  const center = {
    lat: (minLat + maxLat) / 2,
    lng: (minLng + maxLng) / 2,
  };
  const spanLat = Math.abs(maxLat - minLat);
  const spanLng = Math.abs(maxLng - minLng);
  if (spanLat < 0.0005 && spanLng < 0.0005) {
    directoryState.map?.setCenter?.(center);
    directoryState.map?.setZoom?.(8.5);
    return;
  }
  const boundsArray = [[minLng, minLat], [maxLng, maxLat]];
  const boundsObjects = [{ lat: minLat, lng: minLng }, { lat: maxLat, lng: maxLng }];
  const options = { padding: 56, maxZoom: 8.5, duration: 0 };
  try {
    if (typeof directoryState.map?.fitBounds === 'function') {
      directoryState.map.fitBounds(boundsArray, options);
      return;
    }
  } catch {}
  try {
    if (typeof directoryState.map?.fitBounds === 'function') {
      directoryState.map.fitBounds(boundsObjects, options);
      return;
    }
  } catch {}
  try {
    if (window.mappls?.LngLatBounds && typeof directoryState.map?.fitBounds === 'function') {
      const bounds = new window.mappls.LngLatBounds(boundsArray[0], boundsArray[1]);
      directoryState.map.fitBounds(bounds, options);
      return;
    }
  } catch {}
  directoryState.map?.setCenter?.(center);
  directoryState.map?.setZoom?.(getBoundsZoom(spanLat, spanLng));
}

async function renderMapMarkers(vendors) {
  const ready = await ensureMap();
  if (!ready) return;
  clearMapMarkers();
  const points = [];
  for (const vendor of vendors) {
    const point = await geocodeVendor(vendor);
    if (point) points.push({ vendor, point });
  }
  if (!points.length) {
    if (vendors.length) {
      statusEl.textContent = 'Matching innovators are listed below, but no usable coordinates could be derived from the current data yet.';
    }
    fitMapToPoints([]);
    return;
  }
  const groupedPoints = groupMapPoints(points);
  groupedPoints.forEach((entries) => {
    const [{ point }] = entries;
    const ringPoints = createRingPoints(point, entries.length);
    entries.forEach((entry, index) => {
      const marker = new window.mappls.Marker({
        map: directoryState.map,
        position: ringPoints[index],
        html: buildMarkerHtml(entries.length),
        width: entries.length > 1 ? 34 : 20,
        height: entries.length > 1 ? 34 : 20,
        popupHtml: buildPopupHtml([entry]),
        fitbounds: false,
      });
      marker.on?.('click', () => focusVendor(entry.vendor.portal_vendor_id));
      marker.addListener?.('click', () => focusVendor(entry.vendor.portal_vendor_id));
      directoryState.markers.push(marker);
    });
  });
  fitMapToPoints(points);
}

function renderPagination(totalPages, totalMatches) {
  paginationEls.forEach((container) => {
    if (!container) return;
    container.innerHTML = '';
    if (!directoryState.hasSearched || !totalMatches) return;
    container.insertAdjacentHTML('beforeend', `<div class="vendor-page-summary">Showing ${getPageResults().length} of ${totalMatches} results</div>`);
    const prevDisabled = directoryState.currentPage === 1 ? 'disabled' : '';
    container.insertAdjacentHTML('beforeend', `<button class="btn btn-small btn-pagination" data-page-nav="prev" ${prevDisabled}>Prev</button>`);
    const start = Math.max(1, directoryState.currentPage - 2);
    const end = Math.min(totalPages, start + 4);
    for (let page = start; page <= end; page += 1) {
      container.insertAdjacentHTML('beforeend', `<button class="btn btn-small btn-pagination ${page === directoryState.currentPage ? 'active' : ''}" data-page-number="${page}">${page}</button>`);
    }
    const nextDisabled = directoryState.currentPage === totalPages ? 'disabled' : '';
    container.insertAdjacentHTML('beforeend', `<button class="btn btn-small btn-pagination" data-page-nav="next" ${nextDisabled}>Next</button>`);
  });
}

async function renderResults() {
  const totalMatches = directoryState.filteredVendors.length;
  const totalPages = getPageCount();
  const pageVendors = getPageResults();
  const mapVendors = directoryState.hasSearched ? directoryState.filteredVendors : [];
  statusEl.textContent = `Loaded ${directoryState.vendors.length} innovators and ${directoryState.products.length} practices from the synced GRID directory.`;
  setCounts();
  resultsEl.innerHTML = '';
  renderPagination(totalPages, totalMatches);

  if (!directoryState.hasSearched) {
    resultsSummaryEl.textContent = 'Choose an innovator, practice, location, tag, or keyword to search the directory.';
    resultsEl.innerHTML = '<div class="vendor-empty-state">The GRID directory is loaded and ready. Start with one of the dropdown filters or a keyword, then run the search to see matching innovators and practices.</div>';
    await renderMapMarkers([]);
    return;
  }

  if (!totalMatches) {
    resultsSummaryEl.textContent = 'No innovators matched the current filters.';
    resultsEl.innerHTML = '<div class="vendor-empty-state">No GRID innovators match this combination yet. Try a broader location, a different tag, or remove one filter at a time.</div>';
    await renderMapMarkers([]);
    return;
  }

  resultsSummaryEl.textContent = `${totalMatches} innovator result${totalMatches === 1 ? '' : 's'} found. Page ${directoryState.currentPage} of ${totalPages}.`;

  pageVendors.forEach((vendor) => {
    const practicePreview = (vendor.products || []).slice(0, 4).map((product) => product.product_name).filter(Boolean);
    const practiceExtra = Math.max((vendor.products || []).length - practicePreview.length, 0);
    const categoryPreview = uniqueSortedValues((vendor.products || []).flatMap((product) => product.product_categories || [])).slice(0, 4);
    const sixmPreview = uniqueSortedValues((vendor.products || []).flatMap((product) => getEffectiveProductSixM(product))).slice(0, 6);
    resultsEl.insertAdjacentHTML('beforeend', `<article class="vendor-result-card vendor-result-card-fixed" data-vendor-card="${esc(vendor.portal_vendor_id)}"><div class="vendor-result-top"><div><h4>${esc(vendor.vendor_name)}</h4><p>${esc(vendor.location_text || 'Location not listed')}</p></div><span class="admin-badge approved">${esc(String(vendor.products_count || vendor.products?.length || 0))} practices</span></div><div class="btn-group vendor-card-actions"><a class="btn btn-small" href="./vendor-detail.html?vendor=${encodeURIComponent(vendor.portal_vendor_id)}">View Details</a><a class="btn btn-warning btn-small" href="${esc(vendor.portal_vendor_link || '#')}" target="_blank" rel="noreferrer">Open GRID Source</a></div><div class="vendor-card-scroll"><p>${esc(vendor.about_vendor || 'No innovator details available.')}</p><p><strong>District / State:</strong> ${esc([vendor.district, vendor.state].filter(Boolean).join(', ') || 'Not listed')}</p><p><strong>Address:</strong> ${esc(vendor.final_contact_address || vendor.website_address || 'Not listed')}</p><p><strong>Categories:</strong> ${esc(categoryPreview.join(', ') || 'Not listed')}</p><p><strong>6M:</strong> ${esc(sixmPreview.join(', ') || 'Not classified')}</p><p><strong>Practices:</strong> ${esc(practicePreview.join(', ') || 'No practices listed')}${practiceExtra ? ` +${practiceExtra} more` : ''}</p></div></article>`);
  });

  const selectedVendor = directoryState.selectedVendorId && mapVendors.some((vendor) => vendor.portal_vendor_id === directoryState.selectedVendorId)
    ? directoryState.selectedVendorId
    : mapVendors[0]?.portal_vendor_id || null;
  setSelectedVendor(selectedVendor);
  persistSearchState();
  await renderMapMarkers(mapVendors);
}

function applyFilters() {
  const filters = getFilters();
  if (!hasAnyFilter(filters)) {
    directoryState.hasSearched = false;
    directoryState.filteredVendors = [];
    directoryState.currentPage = 1;
    statusEl.textContent = `Loaded ${directoryState.vendors.length} innovators and ${directoryState.products.length} practices from the synced GRID directory.`;
    renderResults();
    return;
  }
  const scored = directoryState.vendors
    .map((vendor) => ({ vendor, score: scoreVendor(vendor, filters) }))
    .filter((entry) => entry.score !== null)
    .sort((left, right) => right.score - left.score || left.vendor.vendor_name.localeCompare(right.vendor.vendor_name))
    .map((entry) => entry.vendor);
  directoryState.hasSearched = true;
  directoryState.filteredVendors = scored;
  directoryState.currentPage = 1;
  persistSearchState();
  renderResults();
}

function clearFilters() {
  [searchEls.supplier, searchEls.product, searchEls.location, searchEls.tags, searchEls.keyword].forEach((input) => { if (input) input.value = ''; });
  setSelectedValues(searchEls.sixm, []);
  directoryState.selectedVendorId = null;
  try { window.sessionStorage.removeItem(SEARCH_STATE_KEY); } catch {}
  applyFilters();
}

async function initializeDirectory() {
  statusEl.textContent = 'Loading GRID directory from Supabase...';
  try {
    const { vendors, products } = await InnovationStore.loadDirectory();
    directoryState.vendors = vendors;
    directoryState.products = products;
    directoryState.filteredVendors = [];
    populateFilterOptions();
    statusEl.textContent = `Loaded ${vendors.length} innovators and ${products.length} practices from the synced GRID directory.`;
    const snapshot = restoreSearchState();
    if (snapshot?.hasSearched) {
      applySearchSnapshot(snapshot);
      const filters = getFilters();
      const scored = directoryState.vendors
        .map((vendor) => ({ vendor, score: scoreVendor(vendor, filters) }))
        .filter((entry) => entry.score !== null)
        .sort((left, right) => right.score - left.score || left.vendor.vendor_name.localeCompare(right.vendor.vendor_name))
        .map((entry) => entry.vendor);
      directoryState.hasSearched = true;
      directoryState.filteredVendors = scored;
      directoryState.currentPage = Math.min(Math.max(1, directoryState.currentPage), Math.max(1, Math.ceil(scored.length / directoryState.pageSize)));
    }
    await renderResults();
  } catch (error) {
    statusEl.textContent = error.message || 'GRID directory could not be loaded.';
    resultsEl.innerHTML = `<article class="admin-card"><p>${esc(statusEl.textContent)}</p></article>`;
  }
}

document.getElementById('run-search').addEventListener('click', applyFilters);
document.getElementById('clear-search').addEventListener('click', clearFilters);
Object.values(searchEls).forEach((input) => {
  if (!input) return;
  input.addEventListener('keypress', (event) => { if (event.key === 'Enter') applyFilters(); });
  input.addEventListener('input', persistSearchState);
  input.addEventListener('change', persistSearchState);
});
resultsEl.addEventListener('click', (event) => {
  if (event.target.closest('a')) return;
  const target = event.target.closest('[data-vendor-card]');
  if (target) {
    setSelectedVendor(target.dataset.vendorCard);
    persistSearchState();
  }
});
paginationEls.forEach((container) => container?.addEventListener('click', (event) => {
  const pageButton = event.target.closest('[data-page-number]');
  if (pageButton) {
    directoryState.currentPage = Number(pageButton.dataset.pageNumber);
    persistSearchState();
    renderResults();
    return;
  }
  const navButton = event.target.closest('[data-page-nav]');
  if (!navButton) return;
  const direction = navButton.dataset.pageNav;
  if (direction === 'prev' && directoryState.currentPage > 1) directoryState.currentPage -= 1;
  if (direction === 'next' && directoryState.currentPage < getPageCount()) directoryState.currentPage += 1;
  persistSearchState();
  renderResults();
}));

initializeDirectory();
