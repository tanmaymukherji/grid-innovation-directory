const loginForm = document.getElementById('loginForm');
const loginStatus = document.getElementById('loginStatus');
const sessionStatus = document.getElementById('sessionStatus');
const sessionPanel = document.getElementById('sessionPanel');
const innovationSyncPanel = document.getElementById('innovationSyncPanel');
const innovationSyncMeta = document.getElementById('innovationSyncMeta');
const innovationSyncRuns = document.getElementById('innovationSyncRuns');
const runInnovationSyncButton = document.getElementById('runInnovationSync');
const signOutButton = document.getElementById('signOutButton');
const adminEditorPanel = document.getElementById('adminEditorPanel');
const adminSearchInput = document.getElementById('adminSearchInput');
const adminSearchMeta = document.getElementById('adminSearchMeta');
const adminSearchResults = document.getElementById('adminSearchResults');
const adminPracticeList = document.getElementById('adminPracticeList');
const adminEditForm = document.getElementById('adminEditForm');
const adminPracticeForm = document.getElementById('adminPracticeForm');
const adminEditorEmpty = document.getElementById('adminEditorEmpty');
const adminEditorFields = document.getElementById('adminEditorFields');
const adminEditStatus = document.getElementById('adminEditStatus');
const adminPracticeEmpty = document.getElementById('adminPracticeEmpty');
const adminPracticeFields = document.getElementById('adminPracticeFields');
const adminPracticeStatus = document.getElementById('adminPracticeStatus');
const saveInnovatorButton = document.getElementById('saveInnovatorButton');
const savePracticeButton = document.getElementById('savePracticeButton');
const productSixMPreview = document.getElementById('productSixMPreview');
const puterModelSelect = document.getElementById('puterModelSelect');
const puterUpdateModeSelect = document.getElementById('puterUpdateMode');
const refreshPuterModelsButton = document.getElementById('refreshPuterModels');
const puterReclassifySixMButton = document.getElementById('puterReclassifySixM');
const puterSuggestPracticeMetadataButton = document.getElementById('puterSuggestPracticeMetadata');
const puterStatus = document.getElementById('puterStatus');

const ADMIN_SESSION_KEY = 'grid-innovation-admin-session';
const SIX_M_OPTIONS = ['Manpower', 'Method', 'Material', 'Machine', 'Money', 'Market'];
const adminState = {
  vendors: [],
  products: [],
  filteredVendors: [],
  selectedVendorId: '',
  selectedProductId: '',
  puterModelsLoaded: false,
  puterModels: [],
};

const editEls = {
  vendorId: document.getElementById('editVendorId'),
  vendorName: document.getElementById('editVendorName'),
  portalContactName: document.getElementById('editPortalContactName'),
  locationText: document.getElementById('editLocationText'),
  district: document.getElementById('editDistrict'),
  state: document.getElementById('editState'),
  pinCode: document.getElementById('editPinCode'),
  agroEcologicalZone: document.getElementById('editAgroEcologicalZone'),
  finalContactEmail: document.getElementById('editFinalContactEmail'),
  finalContactPhone: document.getElementById('editFinalContactPhone'),
  finalContactAddress: document.getElementById('editFinalContactAddress'),
  websiteAddress: document.getElementById('editWebsiteAddress'),
  websiteDetails: document.getElementById('editWebsiteDetails'),
  contactSourceUrl: document.getElementById('editContactSourceUrl'),
  websiteStatus: document.getElementById('editWebsiteStatus'),
  aboutVendor: document.getElementById('editAboutVendor'),
  contactNotes: document.getElementById('editContactNotes'),
};

const editPracticeEls = {
  productId: document.getElementById('editProductId'),
  productName: document.getElementById('editProductName'),
  sourceTags: document.getElementById('editProductSourceTags'),
  aiTags: document.getElementById('editProductAiTags'),
  aiSixm: document.getElementById('editProductAiSixM'),
  aiModel: document.getElementById('editProductAiModel'),
  aiClassifiedAt: document.getElementById('editProductAiClassifiedAt'),
  reviewedTags: document.getElementById('editProductReviewedTags'),
  sixm: document.getElementById('editProductSixM'),
  adminNotes: document.getElementById('editProductAdminNotes'),
  productLink: document.getElementById('editProductLink'),
};

function setStatus(element, message, isError = false) {
  element.textContent = message || '';
  element.classList.toggle('error', Boolean(isError));
}

function escapeHtml(value) {
  return String(value || '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;');
}

function formatDate(value) {
  if (!value) return 'Unknown date';
  return new Date(value).toLocaleString('en-IN', { dateStyle: 'medium', timeStyle: 'short' });
}

function parseCommaList(value) {
  return [...new Set(String(value || '').split(',').map((item) => item.trim()).filter(Boolean))];
}

function normalizeSixMValues(value) {
  const normalizedMap = new Map(SIX_M_OPTIONS.map((item) => [item.toLowerCase(), item]));
  return parseCommaList(value)
    .map((item) => normalizedMap.get(String(item || '').trim().toLowerCase()) || '')
    .filter(Boolean);
}

function renderSixMPreview(value) {
  if (!productSixMPreview) return;
  const items = normalizeSixMValues(value);
  productSixMPreview.innerHTML = items.length
    ? items.map((item) => `<span class="innovation-chip">${escapeHtml(item)}</span>`).join('')
    : '<span class="innovation-chip innovation-chip-muted">No valid 6M categories selected</span>';
}

function setPuterStatus(message, isError = false) {
  if (!puterStatus) return;
  setStatus(puterStatus, message, isError);
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

function parseJsonObjectOrNull(text) {
  try {
    return parseJsonObject(text);
  } catch {
    return null;
  }
}

function extractPuterText(response) {
  if (typeof response === 'string') return response.trim();
  const candidates = [
    response?.message?.content,
    response?.content,
    response?.text,
    response?.result,
    response?.message,
  ];
  for (const candidate of candidates) {
    if (typeof candidate === 'string' && candidate.trim()) return candidate.trim();
    if (Array.isArray(candidate)) {
      const joined = candidate
        .map((item) => {
          if (typeof item === 'string') return item;
          if (typeof item?.text === 'string') return item.text;
          if (typeof item?.content === 'string') return item.content;
          return '';
        })
        .filter(Boolean)
        .join('\n')
        .trim();
      if (joined) return joined;
    }
    if (candidate && typeof candidate === 'object') {
      const nested = String(candidate.text || candidate.content || candidate.message || '').trim();
      if (nested) return nested;
    }
  }
  return JSON.stringify(response || {});
}

function normalizePuterModelEntries(items) {
  return (Array.isArray(items) ? items : [])
    .map((item) => {
      if (typeof item === 'string') return { id: item, name: item };
      const id = String(item?.id || item?.model || item?.name || '').trim();
      const name = String(item?.name || item?.label || item?.id || id).trim();
      return id ? { id, name } : null;
    })
    .filter(Boolean);
}

function getChosenPuterModel() {
  return String(puterModelSelect?.value || '').trim() || null;
}

function getPuterUpdateMode() {
  return String(puterUpdateModeSelect?.value || 'both').trim();
}

function getStoredToken() {
  return window.sessionStorage.getItem(ADMIN_SESSION_KEY) || '';
}

function storeToken(token) {
  if (token) window.sessionStorage.setItem(ADMIN_SESSION_KEY, token);
  else window.sessionStorage.removeItem(ADMIN_SESSION_KEY);
}

function updateSessionUi(isSignedIn) {
  loginForm.style.display = isSignedIn ? 'none' : 'grid';
  sessionPanel.classList.toggle('active', Boolean(isSignedIn));
  innovationSyncPanel.classList.toggle('active', Boolean(isSignedIn));
  adminEditorPanel.classList.toggle('active', Boolean(isSignedIn));
}

function renderInnovationSyncRuns(items) {
  innovationSyncRuns.innerHTML = '';
  if (!items.length) {
    innovationSyncRuns.innerHTML = '<article class="admin-card"><p>No GRID sync runs yet.</p></article>';
    return;
  }
  items.forEach((item) => {
    const card = document.createElement('article');
    card.className = 'admin-card';
    const summary = item.status === 'success'
      ? `${item.vendor_count || 0} innovators and ${item.product_count || 0} practices refreshed`
      : item.error_message || 'No details recorded.';
    card.innerHTML = `<div class="admin-card-header"><h4>${escapeHtml(item.status || 'unknown')}</h4><span class="admin-badge ${item.status === 'success' ? 'approved' : ''}">${escapeHtml(item.status || 'unknown')}</span></div><p><strong>Requested By:</strong> ${escapeHtml(item.requested_by || 'Unknown')}</p><p><strong>Started:</strong> ${escapeHtml(formatDate(item.started_at || item.created_at))}</p><p><strong>Finished:</strong> ${escapeHtml(formatDate(item.finished_at))}</p><p><strong>Summary:</strong> ${escapeHtml(summary)}</p><p><strong>Error:</strong> ${escapeHtml(item.error_message || 'None')}</p></article>`;
    innovationSyncRuns.appendChild(card);
  });
}

function getPracticeRecordsForVendor(vendorId) {
  return adminState.products
    .filter((product) => product.portal_vendor_id === vendorId)
    .sort((left, right) => String(left.product_name || '').localeCompare(String(right.product_name || '')));
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

function getSelectedPractice() {
  return adminState.products.find((item) => item.portal_product_id === adminState.selectedProductId) || null;
}

function buildVendorSearchText(vendor) {
  const linkedProducts = getPracticeRecordsForVendor(vendor.portal_vendor_id);
  const practiceNames = linkedProducts.map((product) => product.product_name).join(' ');
  const practiceTags = linkedProducts.flatMap((product) => getEffectiveProductTags(product)).join(' ');
  const practiceSixM = linkedProducts.flatMap((product) => getEffectiveProductSixM(product)).join(' ');
  const practiceNotes = linkedProducts.map((product) => product.admin_notes || '').join(' ');
  const aiSummaries = linkedProducts.map((product) => product.ai_summary?.summary_of_practice || '').join(' ');
  return [
    vendor.vendor_name,
    vendor.portal_contact_name,
    vendor.location_text,
    vendor.district,
    vendor.state,
    vendor.pin_code,
    vendor.agro_ecological_zone,
    vendor.final_contact_address,
    vendor.final_contact_email,
    vendor.final_contact_phone,
    vendor.about_vendor,
    vendor.contact_notes,
    practiceNames,
    practiceTags,
    practiceSixM,
    practiceNotes,
    aiSummaries,
    (vendor.tags || []).join(' '),
  ].join(' ').toLowerCase();
}

function filterAdminVendors() {
  const query = String(adminSearchInput.value || '').trim().toLowerCase();
  const vendors = [...adminState.vendors].sort((left, right) => String(left.vendor_name || '').localeCompare(String(right.vendor_name || '')));
  adminState.filteredVendors = !query
    ? vendors
    : vendors.filter((vendor) => buildVendorSearchText(vendor).includes(query));
}

function renderAdminResults() {
  adminSearchResults.innerHTML = '';
  if (!adminState.filteredVendors.length) {
    adminSearchResults.innerHTML = '<article class="admin-card"><p>No innovator records matched this search.</p></article>';
    adminSearchMeta.textContent = 'No matching innovator records found.';
    return;
  }

  adminSearchMeta.textContent = `${adminState.filteredVendors.length} innovator record${adminState.filteredVendors.length === 1 ? '' : 's'} found`;
  adminState.filteredVendors.forEach((vendor) => {
    const products = getPracticeRecordsForVendor(vendor.portal_vendor_id).slice(0, 3);
    const card = document.createElement('article');
    card.className = `admin-card admin-search-card${vendor.portal_vendor_id === adminState.selectedVendorId ? ' active' : ''}`;
    card.innerHTML = `<div class="admin-card-header"><h4>${escapeHtml(vendor.vendor_name || 'Unknown Innovator')}</h4><span class="admin-badge approved">${escapeHtml(String(products.length || vendor.products_count || 0))} practices</span></div><p><strong>Location:</strong> ${escapeHtml(vendor.location_text || vendor.final_contact_address || 'Not listed')}</p><p><strong>District/State:</strong> ${escapeHtml([vendor.district, vendor.state].filter(Boolean).join(', ') || 'Not listed')}</p><small>${escapeHtml(products.map((product) => product.product_name).join(' | ') || 'No linked practices listed')}</small>`;
    card.addEventListener('click', () => selectVendor(vendor.portal_vendor_id));
    adminSearchResults.appendChild(card);
  });
}

function setInnovatorEditorVisible(isVisible) {
  adminEditorEmpty.style.display = isVisible ? 'none' : 'block';
  adminEditorFields.classList.toggle('active', Boolean(isVisible));
}

function setPracticeEditorVisible(isVisible) {
  adminPracticeEmpty.style.display = isVisible ? 'none' : 'block';
  adminPracticeFields.classList.toggle('active', Boolean(isVisible));
}

function fillEditor(vendor) {
  editEls.vendorId.value = vendor.portal_vendor_id || '';
  editEls.vendorName.value = vendor.vendor_name || '';
  editEls.portalContactName.value = vendor.portal_contact_name || '';
  editEls.locationText.value = vendor.location_text || '';
  editEls.district.value = vendor.district || '';
  editEls.state.value = vendor.state || '';
  editEls.pinCode.value = vendor.pin_code || '';
  editEls.agroEcologicalZone.value = vendor.agro_ecological_zone || '';
  editEls.finalContactEmail.value = vendor.final_contact_email || '';
  editEls.finalContactPhone.value = vendor.final_contact_phone || '';
  editEls.finalContactAddress.value = vendor.final_contact_address || '';
  editEls.websiteAddress.value = vendor.website_address || '';
  editEls.websiteDetails.value = vendor.website_details || '';
  editEls.contactSourceUrl.value = vendor.contact_source_url || '';
  editEls.websiteStatus.value = vendor.website_status || '';
  editEls.aboutVendor.value = vendor.about_vendor || '';
  editEls.contactNotes.value = vendor.contact_notes || '';
  setInnovatorEditorVisible(true);
}

function renderPracticeList(vendorId) {
  adminPracticeList.innerHTML = '';
  if (!vendorId) {
    adminPracticeList.innerHTML = '<article class="admin-card"><p>Select an innovator to load its practices.</p></article>';
    return;
  }
  const products = getPracticeRecordsForVendor(vendorId);
  if (!products.length) {
    adminPracticeList.innerHTML = '<article class="admin-card"><p>No practice records are linked to this innovator.</p></article>';
    return;
  }
  products.forEach((product) => {
    const card = document.createElement('article');
    card.className = `admin-card admin-search-card${product.portal_product_id === adminState.selectedProductId ? ' active' : ''}`;
    const tags = getEffectiveProductTags(product).slice(0, 6).join(', ') || 'No tags reviewed';
    const sixm = getEffectiveProductSixM(product).join(', ') || 'No 6M set';
    card.innerHTML = `<div class="admin-card-header"><h4>${escapeHtml(product.product_name || 'Untitled practice')}</h4><span class="admin-badge approved">${escapeHtml((product.product_categories || []).join(', ') || 'GRID Practice')}</span></div><p><strong>Tags:</strong> ${escapeHtml(tags)}</p><p><strong>6M:</strong> ${escapeHtml(sixm)}</p><small>${escapeHtml(product.practice_summary || product.product_description || 'No summary saved')}</small>`;
    card.addEventListener('click', () => selectProduct(product.portal_product_id));
    adminPracticeList.appendChild(card);
  });
}

function fillPracticeEditor(product) {
  editPracticeEls.productId.value = product.portal_product_id || '';
  editPracticeEls.productName.value = product.product_name || '';
  editPracticeEls.sourceTags.value = (product.tags || []).join(', ');
  editPracticeEls.aiTags.value = ((product.ai_summary?.tags || []).filter(Boolean)).join(', ');
  editPracticeEls.aiSixm.value = ((product.ai_summary?.six_m_categories || []).filter(Boolean)).join(', ');
  editPracticeEls.aiModel.value = product.ai_model || '';
  editPracticeEls.aiClassifiedAt.value = product.ai_classified_at ? formatDate(product.ai_classified_at) : '';
  editPracticeEls.reviewedTags.value = (product.reviewed_tags || []).join(', ');
  editPracticeEls.sixm.value = (product.six_m_categories || []).join(', ');
  editPracticeEls.adminNotes.value = product.admin_notes || '';
  editPracticeEls.productLink.value = product.product_link || '';
  renderSixMPreview(editPracticeEls.sixm.value);
  setPracticeEditorVisible(true);
  setPuterStatus('Puter AI assist is ready for this practice.');
}

function buildPuterPracticeContext(product) {
  return {
    portal_product_id: product.portal_product_id,
    product_name: product.product_name || null,
    vendor_name: product.vendor_name || null,
    product_location_text: product.product_location_text || null,
    product_categories: product.product_categories || [],
    source_tags: product.tags || [],
    reviewed_tags: product.reviewed_tags || [],
    current_six_m_categories: product.six_m_categories || [],
    ai_summary: product.ai_summary || null,
    practice_summary: product.practice_summary || product.product_description || null,
    innovator_details: product.innovator_details || null,
    practice_details: product.practice_details || null,
    source_reference: product.source_reference || null,
    raw_product: product.raw_product || null,
  };
}

async function ensurePuterModelsLoaded(forceRefresh = false) {
  if (!window.puter?.ai) {
    throw new Error('Puter AI is not available on this page.');
  }
  if (adminState.puterModelsLoaded && !forceRefresh) return adminState.puterModels;
  setPuterStatus('Loading Puter models...');
  const result = await window.puter.ai.listModels();
  const models = normalizePuterModelEntries(result);
  adminState.puterModels = models;
  adminState.puterModelsLoaded = true;
  if (puterModelSelect) {
    const previous = getChosenPuterModel();
    puterModelSelect.innerHTML = '<option value="">Default Puter model</option>';
    models.slice(0, 200).forEach((model) => {
      const option = document.createElement('option');
      option.value = model.id;
      option.textContent = model.name;
      puterModelSelect.appendChild(option);
    });
    if (previous && models.some((model) => model.id === previous)) {
      puterModelSelect.value = previous;
    }
  }
  setPuterStatus(models.length ? `Loaded ${models.length} Puter model options.` : 'No Puter models were returned.');
  return models;
}

async function runPuterChat(prompt) {
  await ensurePuterModelsLoaded(false);
  const model = getChosenPuterModel();
  const options = model ? { model } : {};
  const response = await window.puter.ai.chat(prompt, options);
  return extractPuterText(response);
}

function applyPuterPracticeMetadata(payload, updateMode) {
  if ((updateMode === 'both' || updateMode === 'tags') && Array.isArray(payload.reviewed_tags)) {
    editPracticeEls.reviewedTags.value = parseCommaList(payload.reviewed_tags.join(', ')).join(', ');
  }
  if ((updateMode === 'both' || updateMode === 'sixm') && Array.isArray(payload.six_m_categories)) {
    editPracticeEls.sixm.value = normalizeSixMValues((payload.six_m_categories || []).join(', ')).join(', ');
    renderSixMPreview(editPracticeEls.sixm.value);
  }
  if (payload.admin_notes && !String(editPracticeEls.adminNotes.value || '').trim()) {
    editPracticeEls.adminNotes.value = String(payload.admin_notes).trim();
  }
}

function selectProduct(productId) {
  adminState.selectedProductId = productId || '';
  const product = adminState.products.find((item) => item.portal_product_id === productId);
  if (!product) {
    setPracticeEditorVisible(false);
    renderPracticeList(adminState.selectedVendorId);
    return;
  }
  fillPracticeEditor(product);
  renderPracticeList(product.portal_vendor_id);
  setStatus(adminPracticeStatus, '');
}

function selectVendor(vendorId) {
  adminState.selectedVendorId = vendorId || '';
  const vendor = adminState.vendors.find((item) => item.portal_vendor_id === vendorId);
  if (!vendor) {
    setInnovatorEditorVisible(false);
    adminState.selectedProductId = '';
    renderPracticeList('');
    setPracticeEditorVisible(false);
    return;
  }
  fillEditor(vendor);
  renderAdminResults();
  renderPracticeList(vendor.portal_vendor_id);
  const products = getPracticeRecordsForVendor(vendor.portal_vendor_id);
  const nextProductId = products.some((item) => item.portal_product_id === adminState.selectedProductId)
    ? adminState.selectedProductId
    : (products[0]?.portal_product_id || '');
  if (nextProductId) selectProduct(nextProductId);
  else setPracticeEditorVisible(false);
  setStatus(adminEditStatus, '');
}

async function loadAdminDirectory() {
  if (!getStoredToken()) return;
  adminSearchMeta.textContent = 'Loading innovator records...';
  try {
    const { vendors, products } = await InnovationStore.loadAdminRecords();
    adminState.vendors = Array.isArray(vendors) ? vendors : [];
    adminState.products = Array.isArray(products) ? products : [];
    filterAdminVendors();
    renderAdminResults();
    if (adminState.selectedVendorId && adminState.vendors.some((item) => item.portal_vendor_id === adminState.selectedVendorId)) {
      selectVendor(adminState.selectedVendorId);
    } else {
      adminState.selectedVendorId = '';
      adminState.selectedProductId = '';
      setInnovatorEditorVisible(false);
      setPracticeEditorVisible(false);
      renderPracticeList('');
    }
  } catch (error) {
    adminSearchMeta.textContent = error.message || 'Innovator records could not be loaded.';
    adminSearchResults.innerHTML = '';
  }
}

async function verifySession() {
  const token = getStoredToken();
  if (!token) {
    updateSessionUi(false);
    return false;
  }
  try {
    const data = await InnovationStore.adminRequest('verify', { token });
    if (!data?.valid) throw new Error('Session invalid');
    updateSessionUi(true);
    return true;
  } catch {
    storeToken('');
    updateSessionUi(false);
    innovationSyncMeta.textContent = 'Your admin session has expired. Please sign in again.';
    adminSearchMeta.textContent = 'Your admin session has expired. Please sign in again.';
    return false;
  }
}

async function loadInnovationSyncRuns() {
  const token = getStoredToken();
  if (!token) {
    innovationSyncMeta.textContent = 'Sign in as admin to view and run sync operations.';
    innovationSyncRuns.innerHTML = '';
    return;
  }
  innovationSyncMeta.textContent = 'Loading GRID sync history...';
  try {
    const data = await InnovationStore.adminRequest('listGridSyncRuns', { token });
    const items = Array.isArray(data?.items) ? data.items : [];
    innovationSyncMeta.textContent = `${items.length} GRID sync run${items.length === 1 ? '' : 's'} recorded`;
    renderInnovationSyncRuns(items);
  } catch (error) {
    innovationSyncMeta.textContent = error.message || 'GRID sync history could not be loaded.';
  }
}

async function runInnovationSync() {
  runInnovationSyncButton.disabled = true;
  setStatus(sessionStatus, 'Checking GRID refresh path...');
  try {
    const data = await InnovationStore.adminRequest('syncGridDirectory', { token: getStoredToken() });
    setStatus(sessionStatus, data.message || `Manual sync completed: ${data.vendorCount || 0} innovators and ${data.productCount || 0} practices refreshed in this batch.`);
    await Promise.all([loadInnovationSyncRuns(), loadAdminDirectory()]);
  } catch (error) {
    setStatus(sessionStatus, error.message || 'GRID refresh must be run locally with the importer script.', true);
  } finally {
    runInnovationSyncButton.disabled = false;
  }
}

async function saveInnovatorEdits(event) {
  event.preventDefault();
  const token = getStoredToken();
  const portalVendorId = String(editEls.vendorId.value || '').trim();
  if (!token || !portalVendorId) {
    setStatus(adminEditStatus, 'Select an innovator record first.', true);
    return;
  }

  saveInnovatorButton.disabled = true;
  setStatus(adminEditStatus, 'Saving changes...');
  try {
    const payload = {
      token,
      portalVendorId,
      updates: {
        vendor_name: editEls.vendorName.value,
        portal_contact_name: editEls.portalContactName.value,
        location_text: editEls.locationText.value,
        district: editEls.district.value,
        state: editEls.state.value,
        pin_code: editEls.pinCode.value,
        agro_ecological_zone: editEls.agroEcologicalZone.value,
        final_contact_email: editEls.finalContactEmail.value,
        final_contact_phone: editEls.finalContactPhone.value,
        final_contact_address: editEls.finalContactAddress.value,
        website_details: editEls.websiteDetails.value,
        contact_source_url: editEls.contactSourceUrl.value,
        website_status: editEls.websiteStatus.value,
        about_vendor: editEls.aboutVendor.value,
        contact_notes: editEls.contactNotes.value,
      },
    };
    await InnovationStore.adminRequest('updateGridInnovator', payload);
    setStatus(adminEditStatus, 'Innovator record updated.');
    await loadAdminDirectory();
    selectVendor(portalVendorId);
  } catch (error) {
    setStatus(adminEditStatus, error.message || 'Innovator update failed.', true);
  } finally {
    saveInnovatorButton.disabled = false;
  }
}

async function savePracticeEdits(event) {
  event.preventDefault();
  const token = getStoredToken();
  const portalProductId = String(editPracticeEls.productId.value || '').trim();
  if (!token || !portalProductId) {
    setStatus(adminPracticeStatus, 'Select a practice record first.', true);
    return;
  }

  savePracticeButton.disabled = true;
  setStatus(adminPracticeStatus, 'Saving practice classification...');
  try {
    const payload = {
      token,
      portalProductId,
      updates: {
        reviewed_tags: parseCommaList(editPracticeEls.reviewedTags.value),
        six_m_categories: normalizeSixMValues(editPracticeEls.sixm.value),
        admin_notes: editPracticeEls.adminNotes.value,
      },
    };
    await InnovationStore.adminRequest('updateGridPractice', payload);
    setStatus(adminPracticeStatus, 'Practice classification updated.');
    await loadAdminDirectory();
    selectVendor(adminState.selectedVendorId);
    selectProduct(portalProductId);
  } catch (error) {
    setStatus(adminPracticeStatus, error.message || 'Practice update failed.', true);
  } finally {
    savePracticeButton.disabled = false;
  }
}

async function reclassifyPracticeWithPuter() {
  const product = getSelectedPractice();
  if (!product) {
    setPuterStatus('Select a practice record first.', true);
    return;
  }
  puterReclassifySixMButton.disabled = true;
  setPuterStatus('Asking Puter to reclassify the 6M tags...');
  try {
    const prompt = [
      'Reclassify this GRID practice for an admin-reviewed innovation directory.',
      'Return strict JSON only with this schema:',
      '{"reviewed_tags":string[],"six_m_categories":string[],"admin_notes":string|null}',
      'Rules:',
      '- six_m_categories must only use: Manpower, Method, Material, Machine, Money, Market.',
      '- Apply 6M strictly using these meanings:',
      '- Manpower = trainings or capacity-building support.',
      '- Method = consulting, mentoring, technology transfer, processes, videos, SOPs, manuals, or blogs.',
      '- Market = product/material purchase, market support, or market reports.',
      '- Material = raw material supply.',
      '- Machine = machinery or plant setup.',
      '- Money = financial support.',
      '- Do not assign a 6M category unless the source clearly supports it.',
      '- reviewed_tags should be short, admin-friendly, and useful in search.',
      `- Apply update mode: ${getPuterUpdateMode()}. If mode is sixm, reviewed_tags may stay unchanged.`,
      `Current record:\n${JSON.stringify(buildPuterPracticeContext(product))}`,
    ].join('\n');
    const text = await runPuterChat(prompt);
    const payload = parseJsonObjectOrNull(text);
    if (!payload) throw new Error('Puter returned free text instead of structured JSON. Try another Puter model.');
    applyPuterPracticeMetadata(payload, 'sixm');
    setPuterStatus('AI 6M reclassification applied. Review and save when ready.');
  } catch (error) {
    setPuterStatus(error.message || 'Puter 6M reclassification failed.', true);
  } finally {
    puterReclassifySixMButton.disabled = false;
  }
}

async function suggestPracticeMetadataWithPuter() {
  const product = getSelectedPractice();
  if (!product) {
    setPuterStatus('Select a practice record first.', true);
    return;
  }
  puterSuggestPracticeMetadataButton.disabled = true;
  setPuterStatus('Asking Puter to suggest tags and 6M metadata...');
  try {
    const updateMode = getPuterUpdateMode();
    const prompt = [
      'Improve this GRID practice record for an admin editor.',
      'Return strict JSON only with this schema:',
      '{"reviewed_tags":string[],"six_m_categories":string[],"admin_notes":string|null}',
      'Rules:',
      '- six_m_categories must only use: Manpower, Method, Material, Machine, Money, Market.',
      '- Apply 6M strictly using these meanings:',
      '- Manpower = trainings or capacity-building support.',
      '- Method = consulting, mentoring, technology transfer, processes, videos, SOPs, manuals, or blogs.',
      '- Market = product/material purchase, market support, or market reports.',
      '- Material = raw material supply.',
      '- Machine = machinery or plant setup.',
      '- Money = financial support.',
      '- reviewed_tags should be short, admin-friendly, and useful in search.',
      `- Apply update mode: ${updateMode}. If mode is tags, still return six_m_categories only if clearly inferable. If mode is sixm, still return reviewed_tags if clearly helpful.`,
      `Current record:\n${JSON.stringify(buildPuterPracticeContext(product))}`,
    ].join('\n');
    const text = await runPuterChat(prompt);
    const payload = parseJsonObjectOrNull(text);
    if (!payload) throw new Error('Puter returned free text instead of structured JSON. Try another Puter model.');
    applyPuterPracticeMetadata(payload, updateMode);
    const statusMap = {
      both: 'AI tag and 6M suggestions applied. Review and save when ready.',
      tags: 'AI tag suggestions applied. Review and save when ready.',
      sixm: 'AI 6M suggestions applied. Review and save when ready.',
    };
    setPuterStatus(statusMap[updateMode] || 'AI suggestions applied. Review and save when ready.');
  } catch (error) {
    setPuterStatus(error.message || 'Puter practice metadata assist failed.', true);
  } finally {
    puterSuggestPracticeMetadataButton.disabled = false;
  }
}

loginForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const password = String(document.getElementById('adminPassword').value || '').trim();
  if (!password) {
    setStatus(loginStatus, 'Enter the admin password.', true);
    return;
  }
  setStatus(loginStatus, 'Signing in...');
  try {
    const data = await InnovationStore.adminRequest('login', { password });
    if (!data?.token) throw new Error('Admin login failed.');
    storeToken(data.token);
    document.getElementById('adminPassword').value = '';
    updateSessionUi(true);
    setStatus(loginStatus, 'Signed in successfully.');
    await Promise.all([loadInnovationSyncRuns(), loadAdminDirectory()]);
  } catch (error) {
    setStatus(loginStatus, error.message || 'Admin login failed.', true);
  }
});

signOutButton.addEventListener('click', async () => {
  const token = getStoredToken();
  try {
    if (token) await InnovationStore.adminRequest('logout', { token });
  } catch {}
  storeToken('');
  adminState.vendors = [];
  adminState.products = [];
  adminState.filteredVendors = [];
  adminState.selectedVendorId = '';
  adminState.selectedProductId = '';
  updateSessionUi(false);
  innovationSyncMeta.textContent = 'Sign in as admin to view and run sync operations.';
  adminSearchMeta.textContent = 'Sign in as admin to search and edit innovator records.';
  innovationSyncRuns.innerHTML = '';
  adminSearchResults.innerHTML = '';
  adminPracticeList.innerHTML = '';
  setInnovatorEditorVisible(false);
  setPracticeEditorVisible(false);
  setStatus(sessionStatus, '');
  setStatus(loginStatus, '');
  setStatus(adminEditStatus, '');
  setStatus(adminPracticeStatus, '');
  setPuterStatus('');
});

runInnovationSyncButton.addEventListener('click', async () => { await runInnovationSync(); });
adminSearchInput.addEventListener('input', () => {
  filterAdminVendors();
  renderAdminResults();
});
editPracticeEls.sixm.addEventListener('input', () => {
  renderSixMPreview(editPracticeEls.sixm.value);
});
refreshPuterModelsButton?.addEventListener('click', async () => {
  refreshPuterModelsButton.disabled = true;
  try {
    await ensurePuterModelsLoaded(true);
  } catch (error) {
    setPuterStatus(error.message || 'Puter models could not be loaded.', true);
  } finally {
    refreshPuterModelsButton.disabled = false;
  }
});
puterReclassifySixMButton?.addEventListener('click', reclassifyPracticeWithPuter);
puterSuggestPracticeMetadataButton?.addEventListener('click', suggestPracticeMetadataWithPuter);
adminEditForm.addEventListener('submit', saveInnovatorEdits);
adminPracticeForm.addEventListener('submit', savePracticeEdits);

(async () => {
  const valid = await verifySession();
  if (valid) await Promise.all([loadInnovationSyncRuns(), loadAdminDirectory()]);
  if (window.puter?.ai) {
    setPuterStatus('Puter AI assist is available. Select a practice, then load models or use the default model.');
  } else {
    setPuterStatus('Puter AI did not load on this page.', true);
  }
})();
