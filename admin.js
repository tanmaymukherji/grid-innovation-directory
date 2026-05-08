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

const ADMIN_SESSION_KEY = 'grid-innovation-admin-session';
const SIX_M_OPTIONS = ['Manpower', 'Method', 'Material', 'Machine', 'Money', 'Market'];
const adminState = {
  vendors: [],
  products: [],
  filteredVendors: [],
  selectedVendorId: '',
  selectedProductId: '',
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
  return reviewed.length ? reviewed : (Array.isArray(product?.tags) ? product.tags.filter(Boolean) : []);
}

function buildVendorSearchText(vendor) {
  const linkedProducts = getPracticeRecordsForVendor(vendor.portal_vendor_id);
  const practiceNames = linkedProducts.map((product) => product.product_name).join(' ');
  const practiceTags = linkedProducts.flatMap((product) => getEffectiveProductTags(product)).join(' ');
  const practiceSixM = linkedProducts.flatMap((product) => product.six_m_categories || []).join(' ');
  const practiceNotes = linkedProducts.map((product) => product.admin_notes || '').join(' ');
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
    const sixm = (product.six_m_categories || []).join(', ') || 'No 6M set';
    card.innerHTML = `<div class="admin-card-header"><h4>${escapeHtml(product.product_name || 'Untitled practice')}</h4><span class="admin-badge approved">${escapeHtml((product.product_categories || []).join(', ') || 'GRID Practice')}</span></div><p><strong>Tags:</strong> ${escapeHtml(tags)}</p><p><strong>6M:</strong> ${escapeHtml(sixm)}</p><small>${escapeHtml(product.practice_summary || product.product_description || 'No summary saved')}</small>`;
    card.addEventListener('click', () => selectProduct(product.portal_product_id));
    adminPracticeList.appendChild(card);
  });
}

function fillPracticeEditor(product) {
  editPracticeEls.productId.value = product.portal_product_id || '';
  editPracticeEls.productName.value = product.product_name || '';
  editPracticeEls.sourceTags.value = (product.tags || []).join(', ');
  editPracticeEls.reviewedTags.value = (product.reviewed_tags || []).join(', ');
  editPracticeEls.sixm.value = (product.six_m_categories || []).join(', ');
  editPracticeEls.adminNotes.value = product.admin_notes || '';
  editPracticeEls.productLink.value = product.product_link || '';
  renderSixMPreview(editPracticeEls.sixm.value);
  setPracticeEditorVisible(true);
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
});

runInnovationSyncButton.addEventListener('click', async () => { await runInnovationSync(); });
adminSearchInput.addEventListener('input', () => {
  filterAdminVendors();
  renderAdminResults();
});
editPracticeEls.sixm.addEventListener('input', () => {
  renderSixMPreview(editPracticeEls.sixm.value);
});
adminEditForm.addEventListener('submit', saveInnovatorEdits);
adminPracticeForm.addEventListener('submit', savePracticeEdits);

(async () => {
  const valid = await verifySession();
  if (valid) await Promise.all([loadInnovationSyncRuns(), loadAdminDirectory()]);
})();
