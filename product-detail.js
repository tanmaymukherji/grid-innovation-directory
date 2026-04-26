function esc(value) {
  return String(value || '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;');
}

function renderSpecifications(specifications) {
  if (!Array.isArray(specifications) || !specifications.length) {
    return '<p><strong>Practice Details:</strong> Not listed</p>';
  }
  return `<div><strong>Practice Details</strong><div class="vendor-spec-list">${specifications.map((spec) => `<div class="vendor-spec-item"><strong>${esc(spec.key || 'Field')}</strong>: ${esc(spec.value || 'Not listed')}</div>`).join('')}</div></div>`;
}

function renderLinkList(title, values) {
  const items = Array.isArray(values) ? values.filter(Boolean) : [];
  if (!items.length) return '';
  return `<div class="vendor-inline-list"><strong>${esc(title)}</strong><div>${items.map((item) => `<a href="${esc(item)}" target="_blank" rel="noreferrer">${esc(item)}</a>`).join('<br />')}</div></div>`;
}

async function initProductDetail() {
  const params = new URLSearchParams(window.location.search);
  const productId = params.get('product');
  const root = document.getElementById('product-detail-root');
  if (!productId) {
    root.innerHTML = '<section class="section"><p>Practice id is missing.</p></section>';
    return;
  }
  try {
    const { vendors, products } = await InnovationStore.loadDirectory();
    const product = products.find((item) => item.portal_product_id === productId);
    if (!product) {
      root.innerHTML = '<section class="section"><p>Practice not found in the synced Supabase directory.</p></section>';
      return;
    }
    const vendor = vendors.find((item) => item.portal_vendor_id === product.portal_vendor_id);
    document.getElementById('detail-title').textContent = product.product_name;
    document.getElementById('detail-subtitle').textContent = `${product.vendor_name || 'Unknown Innovator'}${product.product_location_text ? ` • ${product.product_location_text}` : ''}`;
    root.innerHTML = `<section class="section"><div class="vendor-result-top"><div><h3>${esc(product.product_name)}</h3><p>${esc(product.vendor_name || 'Unknown Innovator')}</p></div><span class="admin-badge approved">${esc((product.product_categories || []).join(', ') || 'GRID Practice')}</span></div><div class="vendor-detail-grid"><div>${product.product_image_url ? `<img class="vendor-product-image" src="${esc(product.product_image_url)}" alt="${esc(product.product_name)}" loading="lazy" referrerpolicy="no-referrer" />` : ''}</div><div><p><strong>Summary:</strong> ${esc(product.practice_summary || product.product_description || 'Not listed')}</p><p><strong>Innovator:</strong> ${vendor ? `<a href="./vendor-detail.html?vendor=${encodeURIComponent(vendor.portal_vendor_id)}">${esc(vendor.vendor_name)}</a>` : esc(product.vendor_name || 'Unknown Innovator')}</p><p><strong>Location:</strong> ${esc(product.product_location_text || 'Not listed')}</p><p><strong>Categories:</strong> ${esc((product.product_categories || []).join(', ') || 'Not listed')}</p><p><strong>Tags:</strong> ${esc((product.tags || []).join(', ') || 'Not listed')}</p><p><strong>Reference:</strong> ${esc(product.source_reference || 'Not listed')}</p><p><strong>Source Page:</strong> ${product.product_link ? `<a href="${esc(product.product_link)}" target="_blank" rel="noreferrer">Open GRID source page</a>` : 'Not listed'}</p></div></div>${renderSpecifications(product.product_specifications)}${product.innovator_details ? `<div class="vendor-inline-list"><strong>Details of Innovator</strong><div>${esc(product.innovator_details).replaceAll('\n', '<br />')}</div></div>` : ''}${product.practice_details ? `<div class="vendor-inline-list"><strong>Details of Practice</strong><div>${esc(product.practice_details).replaceAll('\n', '<br />')}</div></div>` : ''}${renderLinkList('Gallery / Images', product.product_gallery_urls)}${renderLinkList('Videos', product.product_video_urls)}${renderLinkList('Attachments / References', product.product_attachment_urls)}</section>`;
  } catch (error) {
    root.innerHTML = `<section class="section"><p>${esc(error.message || 'Practice detail could not be loaded.')}</p></section>`;
  }
}

initProductDetail();
