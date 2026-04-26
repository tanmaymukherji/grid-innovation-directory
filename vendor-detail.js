function esc(value) {
  return String(value || '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;');
}

function renderMediaList(title, values) {
  const items = Array.isArray(values) ? values.filter(Boolean) : [];
  if (!items.length) return '';
  return `<div class="vendor-inline-list"><strong>${esc(title)}</strong><div>${items.map((item) => `<a href="${esc(item)}" target="_blank" rel="noreferrer">${esc(item)}</a>`).join('<br />')}</div></div>`;
}

function getCoverageSummary(vendor) {
  const serviceLocations = Array.isArray(vendor.service_locations) ? vendor.service_locations.filter(Boolean) : [];
  if (serviceLocations.length) return serviceLocations.join(', ');
  return [vendor.district, vendor.state, vendor.country].filter(Boolean).join(', ') || vendor.location_text || 'Innovator details from the synced GRID directory';
}

async function initVendorDetail() {
  const params = new URLSearchParams(window.location.search);
  const vendorId = params.get('vendor');
  const root = document.getElementById('vendor-detail-root');
  if (!vendorId) {
    root.innerHTML = '<section class="section"><p>Innovator id is missing.</p></section>';
    return;
  }
  try {
    const { vendors } = await InnovationStore.loadDirectory();
    const vendor = vendors.find((item) => item.portal_vendor_id === vendorId);
    if (!vendor) {
      root.innerHTML = '<section class="section"><p>Innovator not found in the synced Supabase directory.</p></section>';
      return;
    }
    const coverageSummary = getCoverageSummary(vendor);
    document.getElementById('detail-title').textContent = vendor.vendor_name;
    document.getElementById('detail-subtitle').textContent = coverageSummary;
    root.innerHTML = `<section class="section"><div class="vendor-result-top"><div><h3>${esc(vendor.vendor_name)}</h3><p>${esc(coverageSummary)}</p></div><span class="admin-badge approved">${esc(String(vendor.products_count || vendor.products?.length || 0))} practices</span></div><p>${esc(vendor.about_vendor || 'No innovator details available.')}</p><div class="vendor-detail-grid"><div><h4>Innovator Details</h4><p><strong>Name:</strong> ${esc(vendor.portal_contact_name || vendor.vendor_name || 'Not listed')}</p><p><strong>District:</strong> ${esc(vendor.district || 'Not listed')}</p><p><strong>State:</strong> ${esc(vendor.state || 'Not listed')}</p><p><strong>PIN Code:</strong> ${esc(vendor.pin_code || 'Not listed')}</p><p><strong>Agro-Ecological Zone:</strong> ${esc(vendor.agro_ecological_zone || 'Not listed')}</p></div><div><h4>Source Details</h4><p><strong>Address:</strong> ${esc(vendor.final_contact_address || 'Not listed')}</p><p><strong>Location Text:</strong> ${esc(vendor.location_text || 'Not listed')}</p><p><strong>Status:</strong> ${esc(vendor.website_status || 'Not listed')}</p><p><strong>Source URL:</strong> ${vendor.portal_vendor_link ? `<a href="${esc(vendor.portal_vendor_link)}" target="_blank" rel="noreferrer">Open GRID source page</a>` : 'Not listed'}</p></div></div>${renderMediaList('Images', vendor.innovator_image_urls)}${renderMediaList('Media / Attachments', vendor.innovator_media_urls)}</section><section class="section"><h3>Practices</h3><div class="vendor-products-grid">${(vendor.products || []).length ? (vendor.products || []).map((product) => `<article class="vendor-product-card"><div class="vendor-product-media">${product.product_image_url ? `<img class="vendor-product-image" src="${esc(product.product_image_url)}" alt="${esc(product.product_name)}" loading="lazy" referrerpolicy="no-referrer" />` : ''}<div><h4>${esc(product.product_name)}</h4><p>${esc(product.practice_summary || product.product_description || 'No summary available.')}</p><p><strong>Categories:</strong> ${esc((product.product_categories || []).join(', ') || 'Not listed')}</p><p><strong>Tags:</strong> ${esc((product.tags || []).join(', ') || 'Not listed')}</p><p><strong>Location:</strong> ${esc(product.product_location_text || 'Not listed')}</p></div></div><div class="btn-group"><a class="btn btn-small" href="./product-detail.html?product=${encodeURIComponent(product.portal_product_id)}">View Practice</a><a class="btn btn-warning btn-small" href="${esc(product.product_link || '#')}" target="_blank" rel="noreferrer">Open GRID Source</a></div></article>`).join('') : '<p>No practices were synced for this innovator.</p>'}</div></section>`;
  } catch (error) {
    root.innerHTML = `<section class="section"><p>${esc(error.message || 'Innovator detail could not be loaded.')}</p></section>`;
  }
}

initVendorDetail();
