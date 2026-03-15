/**
 * app.js — Cyber-Intelligence Trading Terminal Frontend Logic
 * ---------------------------------------------------------------------------
 * Integrates Leaflet via CartoDB Dark tile layer and Globe.gl for 3D visualization.
 */

'use strict';

/* ── Constants & Configuration ── */
const API_BASE = ''; // same origin
const ACTIVITY_URL = `${API_BASE}/api/v1/activity`;
const STATS_URL = `${API_BASE}/api/v1/stats`;

// 2D Map Constants
const TILE_URL = 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png';
const TILE_ATTR = '© OSM © CARTO';

// Cyber/Neon Color Palette
const SENTIMENT_COLORS = {
  Positive: '#10b981',  // Emerald 500 equivalent
  Neutral: '#f59e0b',   // Amber 500 equivalent
  Negative: '#ef4444',  // Red 500 equivalent
};

/* ── Global State ── */
let allFeatures = [];
let activeView = 'globe'; // 'globe' or 'map'

// 2D Map Layers
let markerLayer = null;
let heatLayer = null;
let activeMapLayer = 'markers'; // 'markers' | 'heatmap' | 'both'

// Chart instance
let topicChartInstance = null;

// Globe.gl Instance & State
let globeInstance = null;
let globePolygons = []; // Cache GeoJSON polygons
let hoverD = null; // Currently hovered polygon
let switchView = null; // Global view state function

/* ════════════════════════════════════════════════════════════
   1. VIEW SWITCHER LOGIC
   ════════════════════════════════════════════════════════════ */

function initViewSwitcher() {
  const btnGlobe = document.getElementById('btn-view-globe');
  const btnMap = document.getElementById('btn-view-map');
  const globeContainer = document.getElementById('globeViz');
  const mapContainer = document.getElementById('mapViz');

  switchView = (view) => {
    activeView = view;
    if (view === 'globe') {
      btnGlobe.classList.add('active');
      btnMap.classList.remove('active');
      globeContainer.classList.add('active-viz');
      globeContainer.classList.remove('hidden-viz');
      mapContainer.classList.remove('active-viz');
      mapContainer.classList.add('hidden-viz');

      // Resume globe rendering / spinning
      if (globeInstance && globeInstance.controls()) {
        globeInstance.controls().autoRotate = true;
        globeInstance.width(window.innerWidth).height(window.innerHeight);
      }
    } else {
      btnMap.classList.add('active');
      btnGlobe.classList.remove('active');
      mapContainer.classList.add('active-viz');
      mapContainer.classList.remove('hidden-viz');
      globeContainer.classList.remove('active-viz');
      globeContainer.classList.add('hidden-viz');

      // Pause 3D globe processing
      if (globeInstance && globeInstance.controls()) {
        globeInstance.controls().autoRotate = false;
      }

      // Invalidate Leaflet size to fix rendering issues, with delay for DOM reflow
      if (map) {
        setTimeout(() => map.invalidateSize(), 150);
      }
    }
  };

  btnGlobe.addEventListener('click', () => switchView('globe'));
  btnMap.addEventListener('click', () => switchView('map'));

  // Handle window resize for Globe
  window.addEventListener('resize', () => {
    if (activeView === 'globe' && globeInstance) {
      globeInstance.width(window.innerWidth).height(window.innerHeight);
    }
  });
}

/* ════════════════════════════════════════════════════════════
   2. 3D GLOBE INITIALIZATION (Globe.gl)
   ════════════════════════════════════════════════════════════ */

function initGlobe() {
  const container = document.getElementById('globeViz');

  // Dynamic Live Pings generator
  const arcTypes = ['Trade', 'Diplomatic', 'Military'];
  function generateLivePings() {
    const N_ARCS = 6 + Math.floor(Math.random() * 3); // 6 to 8 connections
    return [...Array(N_ARCS).keys()].flatMap(() => {
      const type = arcTypes[Math.floor(Math.random() * arcTypes.length)];
      let color = '#ef4444'; // Red 500 for Military
      if (type === 'Trade') color = '#10b981'; // Emerald 500
      if (type === 'Diplomatic') color = '#f59e0b'; // Amber 500

      const coords = {
        startLat: (Math.random() - 0.5) * 160,
        startLng: (Math.random() - 0.5) * 360,
        endLat: (Math.random() - 0.5) * 160,
        endLng: (Math.random() - 0.5) * 360,
        type: type
      };

      return [
        // The Track
        { ...coords, color: 'rgba(255,255,255,0.1)', stroke: 0.2, dashLength: 1, dashGap: 0, animated: false },
        // The Pulse
        { ...coords, color: color, stroke: 0.3, dashLength: 0.15, dashGap: 0.85, animated: true }
      ];
    });
  }

  globeInstance = Globe()(container)
    .backgroundColor('rgba(5, 10, 25, 0.8)')
    .showAtmosphere(true)
    .atmosphereColor('rgba(10, 25, 50, 0.6)')
    .backgroundImageUrl('https://cdn.jsdelivr.net/npm/three-globe/example/img/night-sky.png')  // The stars go in the background
    .globeImageUrl('https://cdn.jsdelivr.net/npm/three-globe/example/img/earth-night.jpg')     // The dark earth goes on the globe
    .bumpImageUrl('https://cdn.jsdelivr.net/npm/three-globe/example/img/earth-topology.png')   // The topology makes the mountains 3D
    .pointRadius(0.5)
    .pointAltitude(0.01)
    .pointColor('color')
    .pointLabel('label')
    .arcsData(generateLivePings())
    .arcStartLat(d => d.startLat)
    .arcStartLng(d => d.startLng)
    .arcEndLat(d => d.endLat)
    .arcEndLng(d => d.endLng)
    .arcColor(d => d.color)
    .arcStroke(d => d.stroke)
    .arcDashLength(d => d.dashLength)
    .arcDashGap(d => d.dashGap)
    .arcDashAnimateTime(d => d.animated ? 4000 : 0)
    .polygonAltitude(d => d === hoverD ? 0.06 : 0.01)
    .polygonSideColor(() => 'rgba(0, 0, 0, 0.2)')
    .polygonStrokeColor(d => d === hoverD ? '#ffffff' : 'rgba(0, 0, 0, 0.5)')
    .polygonCapColor(d => {
      console.log('Checking Color For:', d.properties.NAME, 'Sentiment Count:', d.properties._sentimentCount, 'Status:', d.properties._sentimentStatus);
      if (d === hoverD) return 'rgba(16, 185, 129, 0.4)'; // Hover Translucent Emerald

      // Check for zero-data and Antarctica / Greenland
      if (d.properties.NAME === 'Antarctica' || d.properties.NAME === 'Greenland' || d.properties._sentimentCount === 0 || d.properties._sentimentStatus === 'nodata') {
        return 'rgba(30, 41, 59, 0.4)'; // Default No Data (Slate 800 but darker to match ocean)
      }

      // Dynamic color based on net sentiment (computed in updateGlobeData)
      if (d.properties._sentimentStatus === 'positive') return 'rgba(16, 185, 129, 0.8)'; // Emerald 500
      if (d.properties._sentimentStatus === 'stable') return 'rgba(56, 189, 248, 0.8)'; // Light Blue
      if (d.properties._sentimentStatus === 'caution') return 'rgba(245, 158, 11, 0.8)'; // Amber 500
      if (d.properties._sentimentStatus === 'negative') return 'rgba(239, 68, 68, 0.8)';  // Red 500

      return 'rgba(30, 41, 59, 0.6)'; // Default No Data (Slate 800)
    })
    .onPolygonHover(hover => {
      hoverD = hover;
      // Trigger update of altitude and color
      globeInstance
        .polygonAltitude(globeInstance.polygonAltitude())
        .polygonStrokeColor(globeInstance.polygonStrokeColor())
        .polygonCapColor(globeInstance.polygonCapColor());
    });

  // Auto-rotate setup
  globeInstance.controls().autoRotate = true;
  globeInstance.controls().autoRotateSpeed = 1.0;

  // Custom appearance tweaks
  const globeMaterial = globeInstance.globeMaterial();
  globeMaterial.color.set('#2a2a35'); // Darken the earth base further

  // Load GeoJSON countries dataset
  fetch('https://raw.githubusercontent.com/vasturiano/globe.gl/master/example/datasets/ne_110m_admin_0_countries.geojson')
    .then(res => res.json())
    .then(countries => {
      globePolygons = countries.features;
      globeInstance.polygonsData(globePolygons);

      // Also add GeoJSON to 2D Map
      initMapGeoJSON(globePolygons);
    });

  // Add Click interaction
  globeInstance.onPolygonClick(polygon => {
    console.log('GEOJSON MAP NAME:', polygon.properties.ADMIN || polygon.properties.NAME);
    // 1. Switch View to map by simulating the native button toggle
    const mapBtn = document.getElementById('btn-view-map');
    if (mapBtn) mapBtn.click();

    // 2. Open Intel Panel immediately so UI responds fast
    handleCountryClick(polygon.properties);

    // 3. Center map to polygon bounds (with a delay to allow Leaflet invalidateSize)
    if (map && polygon) {
      setTimeout(() => {
        try {
          const bounds = L.geoJSON(polygon).getBounds();
          map.fitBounds(bounds, { padding: [50, 50], maxZoom: 5 });
        } catch (e) {
          console.error("Could not bound map to polygon", e);
        }
      }, 150);
    }
  });

  setInterval(() => {
    globeInstance.arcsData(generateLivePings());
  }, 4000);

  console.log("3D Globe Initialized");
}

let mapGeoJsonLayer = null;

function initMapGeoJSON(polygons) {
  if (mapGeoJsonLayer) map.removeLayer(mapGeoJsonLayer);

  mapGeoJsonLayer = L.geoJSON(polygons, {
    style: function (feature) {
      let status = feature.properties._sentimentStatus;
      let count = feature.properties._sentimentCount || 0;
      let name = feature.properties.NAME;
      let color = 'rgba(10, 20, 30, 0.4)';
      let border = 'rgba(255, 255, 255, 0.1)';

      if (name === 'Antarctica' || name === 'Greenland' || count === 0 || status === 'nodata') {
        color = 'rgba(10, 20, 30, 0.4)';
      } else {
        if (status === 'positive') color = 'rgba(16, 185, 129, 0.8)'; // Emerald 500
        if (status === 'stable') color = 'rgba(56, 189, 248, 0.8)'; // Light Blue
        if (status === 'caution') color = 'rgba(245, 158, 11, 0.8)'; // Amber 500
        if (status === 'negative') color = 'rgba(239, 68, 68, 0.8)';  // Red 500
      }

      return {
        fillColor: color,
        weight: 1,
        color: border,
        fillOpacity: 1
      };
    },
    onEachFeature: function (feature, layer) {
      layer.on('click', () => {
        handleCountryClick(feature.properties);
      });
    }
  }).addTo(map);

  // Keep markers above the polygons
  if (markerLayer) markerLayer.bringToFront();
}

/* ════════════════════════════════════════════════════════════
   2.5 INTEL PANEL & DATA BINDING LOGIC
   ════════════════════════════════════════════════════════════ */

function initUIPanels() {
  const leftPanel = document.getElementById('leftFiltersPanel');
  const filterToggleBtn = document.getElementById('filter-toggle-wrapper');
  const rightPanel = document.getElementById('countryIntelPanel');

  document.getElementById('btn-toggle-filters').addEventListener('click', () => {
    // If it's already open, toggle it closed
    if (!leftPanel.classList.contains('collapsed')) {
      leftPanel.classList.add('collapsed');
      document.body.classList.remove('filters-open');
    } else {
      leftPanel.classList.remove('collapsed');
      document.body.classList.add('filters-open');
    }
  });

  document.getElementById('closeIntelPanel').addEventListener('click', () => {
    rightPanel.classList.add('collapsed');
    document.body.classList.remove('intel-open');
  });
}

const COUNTRY_MAP = {
  "USA": "United States of America",
  "United States": "United States of America",
  "US": "United States of America",
  "China": "China",
  "Mainland China": "China",
  "UK": "United Kingdom",
  "Great Britain": "United Kingdom",
  "Russia": "Russia"
};

function handleCountryClick(countryProp) {
  const cName = countryProp.ADMIN || countryProp.name || 'UNKNOWN ZONE';
  const cStatus = countryProp._sentimentStatus || 'nodata';
  
  console.log('Clicked:', cName);

  // Update header and status
  document.getElementById('selectedCountryName').textContent = cName.toUpperCase();
  const statusEl = document.getElementById('countrySentiment');
  statusEl.textContent = cStatus.toUpperCase();

  if (cStatus === 'positive') statusEl.style.color = 'var(--neon-green)';
  else if (cStatus === 'negative') statusEl.style.color = 'var(--neon-red)';
  else if (cStatus === 'neutral') statusEl.style.color = 'var(--neon-blue)';
  else statusEl.style.color = 'var(--text-muted)';

  // Slide open the panel
  document.getElementById('countryIntelPanel').classList.remove('collapsed');
  document.body.classList.add('intel-open');

  const feedCont = document.getElementById('countryFeed');
  feedCont.innerHTML = ''; // First, clear the container

  // Filter features to just this country (using strictly geo-filtering)
  const matchedFeatures = allFeatures.filter(post => {
    let loc = post.properties?.geocoded_location;
    if (loc && COUNTRY_MAP[loc]) {
        loc = COUNTRY_MAP[loc];
    }
    const txt = post.properties?.text || post.properties?.source_text;

    return (loc && loc.toLowerCase().includes(cName.toLowerCase())) ||
      (txt && txt.toLowerCase().includes(cName.toLowerCase()));
  });

  if (matchedFeatures.length === 0) {
    feedCont.innerHTML = `<div style="padding: 20px; color: #888;">No local intercepts detected for ${escapeHtml(cName)}.</div>`;
    return;
  }

  // Build feed cards
  feedCont.innerHTML = matchedFeatures.map(f => {
    const props = f.properties || {};
    const topic = props.topic ? escapeHtml(props.topic).toUpperCase() : 'GLOBAL_INTEL';
    const sentiment = props.sentiment_label || 'Neutral';
    const score = props.sentiment_score !== undefined ? parseFloat(props.sentiment_score).toFixed(2) : 'N/A';
    const text = props.text || props.source_text || '[ENCRYPTED]';
    const color = SENTIMENT_COLORS[sentiment] || SENTIMENT_COLORS.Neutral;
    const timeStr = props.processed_at ? new Date(props.processed_at).toLocaleTimeString() : '';
    const sourceText = props.source ? props.source.toUpperCase() : 'SYS_NODE';

    return `
      <div class="feed-card" style="border-left: 3px solid ${color}">
        <div class="feed-header">
          <span class="feed-topic" style="color: ${color}">#${topic}</span>
          <span class="feed-sentiment" style="color: ${color}">Score: ${score}</span>
        </div>
        <div class="feed-body">${escapeHtml(text)}</div>
        <div class="feed-footer">
          <span class="source-tag" style="color: var(--neon-blue); font-weight: bold;">[ ${escapeHtml(sourceText)} ]</span>
          <span>${timeStr}</span>
        </div>
      </div>
    `;
  }).join('');
}

function updateGlobeData(features) {
  if (!globeInstance) return;

  // 1. Aggregate sentiment per country
  // Reset previous scores
  globePolygons.forEach(p => {
    p.properties._sentimentTotal = 0;
    p.properties._sentimentCount = 0;
    p.properties._sentimentStatus = 'nodata';
  });

function getNormalizedName(loc) {
  if (!loc) return "";
  const n = loc.trim().toLowerCase();
  const map = {
    "usa": "united states of america", "us": "united states of america", "united states": "united states of america",
    "china": "china", "prc": "china",
    "russia": "russia", "russian federation": "russia",
    "uk": "united kingdom", "great britain": "united kingdom"
  };
  return map[n] || n;
}

  features.forEach(f => {
    let loc = f.properties?.geocoded_location || '';
    const sentiment = f.properties?.sentiment_label;
    const score = parseFloat(f.properties?.sentiment_score || 0);

    if (!loc || !sentiment) return;

    const searchLoc = getNormalizedName(loc);

    // Strict match location string against GeoJSON properties (ADMIN, NAME, etc)
    const match = globePolygons.find(p => {
      const admin = (p.properties.ADMIN || p.properties.NAME || '').toLowerCase();
      if (!admin) return false;
      // EXACT match first
      if (admin === searchLoc) return true; 
      
      // Fallback with word boundaries to prevent Niger/Nigeria collisions
      try {
        const safeAdmin = admin.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const adminRegex = new RegExp(`\\b${safeAdmin}\\b`, 'i');
        if (adminRegex.test(searchLoc)) return true;
        
        if (searchLoc.length > 4) {
          const safeSearchLoc = searchLoc.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
          const searchRegex = new RegExp(`\\b${safeSearchLoc}\\b`, 'i');
          if (searchRegex.test(admin)) return true;
        }
      } catch (e) {}
      
      return false;
    });

    if (match) {
      match.properties._sentimentTotal += score;
      match.properties._sentimentCount += 1;
    }
  });

  // Calculate Net Status with widened Neutral threshold
  globePolygons.forEach(p => {
    const count = p.properties._sentimentCount;
    if (count === 0) {
      p.properties._sentimentStatus = 'nodata';
    } else {
      const avgScore = p.properties._sentimentTotal / count;
      if (avgScore === 0) {
        p.properties._sentimentStatus = 'nodata';
      } else if (avgScore >= 0.2) {
        p.properties._sentimentStatus = 'positive';
      } else if (avgScore >= 0.0 && avgScore < 0.2) {
        p.properties._sentimentStatus = 'stable';
      } else if (avgScore >= -0.2 && avgScore < 0.0) {
        p.properties._sentimentStatus = 'caution';
      } else {
        p.properties._sentimentStatus = 'negative';
      }
    }
  });

  // Re-trigger polygon shading for 3D globe and 2D map
  if (globePolygons.length > 0) {
    // Soft update: just re-trigger color accessors, don't recreate all WebGL geometry
    globeInstance.polygonCapColor(globeInstance.polygonCapColor());

    if (typeof mapGeoJsonLayer !== 'undefined' && mapGeoJsonLayer) {
      initMapGeoJSON(globePolygons); // Rebuild styles
    }
    
    // Force WebGL geometry/material refresh
    globeInstance.polygonsData(globePolygons);
  }

  // 2. Map GeoJSON points to format expected by Globe.gl for markers
  const pointsData = features.map(f => {
    const lat = f.geometry.coordinates[1];
    const lng = f.geometry.coordinates[0];
    const sentiment = f.properties?.sentiment_label || 'Neutral';
    const color = SENTIMENT_COLORS[sentiment] || SENTIMENT_COLORS.Neutral;

    // Popup label string
    const topic = f.properties?.topic || 'SYSTEM_EVENT';
    const text = f.properties?.text || f.properties?.source_text || '';
    const label = `
      <div style="background: rgba(5,5,5,0.8); border: 1px solid ${color}; padding: 8px; border-radius: 4px; color: #fff; font-family: 'Share Tech Mono', monospace; font-size: 12px; max-width: 250px;">
        <div style="color: ${color}; font-weight: bold; margin-bottom: 4px;">[${topic.toUpperCase()}] // ${sentiment.toUpperCase()}</div>
        <div style="white-space: normal;">${escapeHtml(text)}</div>
      </div>
    `;

    return {
      lat,
      lng,
      size: Math.abs(f.properties?.sentiment_score || 0.3) * 1.5,
      color,
      label
    };
  });

  globeInstance.pointsData(pointsData);
}


/* ════════════════════════════════════════════════════════════
   3. 2D MAP INITIALIZATION (Leaflet)
   ════════════════════════════════════════════════════════════ */

const map = L.map('map', {
  center: [20, 10],
  zoom: 2,
  minZoom: 2,
  zoomControl: false,
  attributionControl: true,
});

L.tileLayer(TILE_URL, {
  attribution: TILE_ATTR,
  subdomains: 'abcd',
  maxZoom: 19,
}).addTo(map);

// Custom zoom control — bottom-right
L.control.zoom({ position: 'bottomright' }).addTo(map);

/* ── 2D Map Helpers ── */

function sentimentCircle(feature) {
  const label = feature.properties?.sentiment_label || 'Neutral';
  const color = SENTIMENT_COLORS[label] || SENTIMENT_COLORS.Neutral;
  return L.circleMarker(
    [feature.geometry.coordinates[1], feature.geometry.coordinates[0]],
    {
      radius: 6,
      fillColor: color,
      color: '#050505',
      weight: 1.5,
      opacity: 1,
      fillOpacity: 0.9,
    }
  );
}

function buildPopupHTML(props) {
  const source = (props.source || 'SYS_NODE').toUpperCase();
  const sentiment = (props.sentiment_label || 'NEUTRAL').toUpperCase();
  const score = props.sentiment_score != null ? props.sentiment_score.toFixed(3) : '—';
  let rawText = props.text || props.source_text || '[NO SIGNAL DATA]';
  const location = props.geocoded_location || 'UNKNOWN SECTOR';
  const processed = props.processed_at
    ? new Date(props.processed_at).toLocaleString()
    : 'T-MINUS UNKNOWN';
  let escapedText = escapeHtml(rawText);

  const color = SENTIMENT_COLORS[props.sentiment_label] || SENTIMENT_COLORS.Neutral;

  let finalHtml = '';
  if (props.topic) {
    finalHtml += `<strong style="color: var(--neon-blue)">// CATEGORY: ${escapeHtml(props.topic).toUpperCase()}</strong><br/>`;
  }
  finalHtml += `<div style="margin-top: 8px;">> ${escapedText}</div>`;

  return `
    <div class="popup-content">
      <div class="popup-header">
        <span class="popup-source" style="color: #fff; font-weight: bold;">[${source}]</span>
        <span class="popup-sentiment" style="color: ${color}; font-weight: bold;">STAT: ${sentiment}</span>
      </div>
      <p class="popup-text" style="font-family: 'Share Tech Mono', monospace; font-size: 13px;">${finalHtml}</p>
      <div class="popup-meta" style="font-family: 'Share Tech Mono', monospace;">
        <div class="popup-meta-row">
          <strong>LOC_HASH:</strong> ${escapeHtml(location)}
        </div>
        <div class="popup-meta-row">
          <strong>SIG_STRENGTH:</strong>
          <span class="popup-score" style="color:${color}">${score}</span>
        </div>
        <div class="popup-meta-row">
          <strong>TIMESTAMP:</strong> ${processed}
        </div>
      </div>
    </div>
  `;
}

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/* ── 2D Layer Builders ── */

function buildMarkerLayer(features) {
  const group = L.markerClusterGroup({
    chunkedLoading: true,
    maxClusterRadius: 50,
    showCoverageOnHover: false,
    iconCreateFunction(cluster) {
      const count = cluster.getChildCount();
      // Cyber cluster style
      return L.divIcon({
        html: `<div style="background: rgba(0, 240, 255, 0.2); border: 1px solid var(--neon-blue); border-radius: 50%; width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; color: var(--neon-blue); font-weight: bold; box-shadow: 0 0 10px rgba(0,240,255,0.3);"><span style="text-shadow: 0 0 3px var(--neon-blue);">${count}</span></div>`,
        className: 'marker-cluster cyber-cluster',
        iconSize: L.point(40, 40),
      });
    },
  });

  features.forEach(feature => {
    const marker = sentimentCircle(feature);
    marker.bindPopup(buildPopupHTML(feature.properties), {
      maxWidth: 320,
      className: 'coriolis-popup',
    });
    group.addLayer(marker);
  });

  return group;
}

function buildHeatLayer(features) {
  const points = features.map(f => {
    const [lng, lat] = f.geometry.coordinates;
    const intensity = Math.max(0.3, Math.abs(f.properties?.sentiment_score || 0.3));
    return [lat, lng, intensity];
  });

  return L.heatLayer(points, {
    radius: 20,
    blur: 15,
    maxZoom: 8,
    gradient: {
      0.0: '#0000ff',
      0.25: '#00f0ff',
      0.5: '#00ff00',
      0.75: '#ffff00',
      1.0: '#ff003c',
    },
  });
}

function removeAllLayers() {
  if (markerLayer) { map.removeLayer(markerLayer); }
  if (heatLayer) { map.removeLayer(heatLayer); }
}

function applyDisplay(features, displayMode) {
  removeAllLayers();

  markerLayer = buildMarkerLayer(features);
  heatLayer = buildHeatLayer(features);

  if (displayMode === 'markers' || displayMode === 'both') {
    map.addLayer(markerLayer);
  }
  if (displayMode === 'heatmap' || displayMode === 'both') {
    map.addLayer(heatLayer);
  }
}

/* ════════════════════════════════════════════════════════════
   4. FILTER & DATA MANAGER
   ════════════════════════════════════════════════════════════ */

function getFilteredFeatures() {
  const srcFilter = document.getElementById('filter-source').value.toLowerCase();
  const sentFilter = document.getElementById('filter-sentiment').value;

  return allFeatures.filter(f => {
    const props = f.properties || {};
    const src = (props.source || props.source_type || '').toLowerCase();
    const sent = props.sentiment_label || '';

    if (srcFilter && src !== srcFilter) return false;
    if (sentFilter && sent !== sentFilter) return false;
    return true;
  });
}

function applyFilters() {
  const filtered = getFilteredFeatures();

  // Update Maps
  applyDisplay(filtered, activeMapLayer);
  updateGlobeData(filtered);

  // Show visible marker count briefly in status dot tooltip
  document.getElementById('status-dot').title = `SYSTEM GRID HOLDING ${filtered.length} NODES`;
}

/* ════════════════════════════════════════════════════════════
   5. STATS & TICKER PANEL UI
   ════════════════════════════════════════════════════════════ */

async function loadStats() {
  try {
    const url = new URL(STATS_URL, window.location.origin);
    const topic = document.getElementById('filter-topic')?.value;
    const search = document.getElementById('filter-search')?.value;
    const timeRange = document.getElementById('filter-time')?.value;

    if (topic) url.searchParams.set('topic', topic);
    if (search) url.searchParams.set('search', search);
    if (timeRange) url.searchParams.set('time_range', timeRange);

    const res = await fetch(url.toString());
    const data = await res.json();

    document.getElementById('stat-total').textContent =
      (data.total_records ?? '0').toLocaleString();
    document.getElementById('stat-geo').textContent =
      (data.records_with_coords ?? '0').toLocaleString();
    document.getElementById('stat-pos').textContent =
      (data.by_sentiment_label?.Positive ?? 0).toLocaleString();
    document.getElementById('stat-neg').textContent =
      (data.by_sentiment_label?.Negative ?? 0).toLocaleString();

    // Top locations (Hot Zones)
    const ul = document.getElementById('top-locations');
    ul.innerHTML = '';
    const locations = data.top_geocoded_locations || [];

    if (locations.length === 0) {
      ul.innerHTML = '<li class="loading-text">[NO ZONES DETECTED]</li>';
    } else {
      locations.slice(0, 5).forEach(item => {
        const li = document.createElement('li');
        li.innerHTML = `
          <span class="location-name" title="${escapeHtml(item.location)}">> ${escapeHtml(item.location)}</span>
          <span class="location-count text-neon">[${item.count}]</span>
        `;
        ul.appendChild(li);
      });
    }

    // Update Topic Donut Chart
    const topicCounts = data.by_topic || {};
    const chartCtx = document.getElementById('topicChart')?.getContext('2d');
    if (chartCtx) {
      const labels = Object.keys(topicCounts).map(t => t.toUpperCase());
      const chartData = Object.values(topicCounts);

      if (topicChartInstance) {
        topicChartInstance.data.labels = labels;
        topicChartInstance.data.datasets[0].data = chartData;
        topicChartInstance.update();
      } else {
        topicChartInstance = new Chart(chartCtx, {
          type: 'doughnut',
          data: {
            labels: labels,
            datasets: [{
              data: chartData,
              backgroundColor: [
                '#00f0ff', '#00ff00', '#ff003c', '#ffff00', '#ff00ff',
                '#ff8a00', '#0066ff', '#8b949e'
              ],
              borderWidth: 1,
              borderColor: '#050505',
              hoverOffset: 4
            }]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '75%',
            plugins: {
              legend: { display: false },
              tooltip: {
                theme: 'dark',
                backgroundColor: 'rgba(5, 5, 5, 0.95)',
                borderColor: 'var(--neon-blue)',
                borderWidth: 1,
                titleFont: { family: 'Share Tech Mono', size: 12 },
                bodyFont: { family: 'Share Tech Mono', size: 14 },
                callbacks: {
                  label: function (context) {
                    return ` [${context.label}] : ${context.raw} PINGS`;
                  }
                }
              }
            }
          }
        });
      }
    }

  } catch (err) {
    console.warn('System Stats Error:', err);
  }
}

function updateLiveTicker(features) {
  const container = document.getElementById('live-ticker');
  if (!container) return;

  // Get 10 most recent (exclude Social/Mastodon data for main News Ticker)
  const recent = features
    .filter(f => (f.properties?.source || '').toLowerCase() !== 'mastodon')
    .slice(0, 10);

  if (recent.length === 0) {
    container.innerHTML = `<span class="ticker-item text-muted">[NO RECENT DATA IN DATASTREAM]</span>`;
    return;
  }

  container.innerHTML = recent.map(f => {
    const props = f.properties || {};
    const topic = props.topic ? escapeHtml(props.topic).toUpperCase() : 'SYSTEM';
    const sentiment = props.sentiment_label || 'Neutral';
    const text = props.text || props.source_text || '[ENCRYPTED]';
    const color = SENTIMENT_COLORS[sentiment] || SENTIMENT_COLORS.Neutral;

    // Truncate text for ticker
    const shortText = text.length > 80 ? text.substring(0, 80) + '...' : text;

    return `
      <span class="ticker-item" style="display: flex; gap: 8px; align-items: center;">
        <span style="color: ${color}; font-weight: bold;">[${topic}]</span>
        <span style="border-right: 1px solid rgba(0,240,255,0.3); padding-right: 10px; margin-right: 10px;">
          ${escapeHtml(shortText)}
        </span>
      </span>
    `;
  }).join('');
}

function updateLiveInterceptor(features) {
  const container = document.getElementById('interceptor-feed');
  if (!container) return;

  // Get 5 most recent titles (heavily favor Social/Mastodon raw pings for Local Interceptor)
  const recent = features
    .filter(f => (f.properties?.source || '').toLowerCase() === 'mastodon')
    .slice(0, 5);
  
  if (recent.length === 0) {
    container.innerHTML = `<div class="ticker-item text-muted">[NO RECENT DATA]</div>`;
    return;
  }

  container.innerHTML = recent.map(f => {
    const text = f.properties?.text || f.properties?.source_text || '[ENCRYPTED]';
    // Snip short clean titles from full text
    const summaryStr = text.length > 55 ? text.substring(0, 55).replace('\n', ' ') + '...' : text.replace('\n', ' ');
    const url = f.properties?.url || f.properties?.source_url || '#';
    const loc = f.properties?.geocoded_location || 'UNKNOWN';
    return `<div class="interceptor-item" onclick="window.handleHotZoneClick('${escapeHtml(loc)}')" style="color: inherit;">> ${escapeHtml(summaryStr)}</div>`;
  }).join('');
}

window.handleHotZoneClick = (countryName) => {
  const searchName = countryName.toLowerCase();
  const p = globePolygons.find(x => (x.properties.ADMIN || x.properties.name || '').toLowerCase() === searchName);
  if (p) {
    handleCountryClick(p.properties);
  } else {
    handleCountryClick({ ADMIN: countryName, _sentimentStatus: 'nodata' });
  }
};

function updateHotZones() {
  const container = document.getElementById('hot-zones-list');
  if (!container) return;

  // Slice top 5 polygons by data volume
  const topCountries = [...globePolygons]
    .filter(p => (p.properties._sentimentCount || 0) > 0)
    .sort((a, b) => (b.properties._sentimentCount || 0) - (a.properties._sentimentCount || 0))
    .slice(0, 5);

  if (topCountries.length === 0) {
    container.innerHTML = `<li class="text-muted" style="font-size: var(--text-sm)">[AWAITING_ACTIVITY]</li>`;
    return;
  }

  const UI_NAMES = {
    "United States of America": "USA",
    "Russian Federation": "RUSSIA",
    "United Kingdom": "UK",
    "People's Republic of China": "CHINA"
  };

  container.innerHTML = topCountries.map((p, index) => {
    const name = p.properties.ADMIN || p.properties.name || 'UNKNOWN';
    let displayName = UI_NAMES[name] || name;
    displayName = displayName.toUpperCase();
    const num = String(index + 1).padStart(2, '0');
    const count = p.properties._sentimentCount || 0;
    return `
      <li onclick="window.handleHotZoneClick('${escapeHtml(name)}')" style="cursor: pointer; font-size: var(--text-sm); transition: color 0.2s;" onmouseover="this.style.color='var(--neon-green)'" onmouseout="this.style.color=''">
        <div class="hot-zone-row">
          <span class="hz-name"><span class="live-dot" style="display:inline-block; margin-right: 6px; background-color: var(--neon-red); box-shadow: 0 0 5px var(--neon-red);"></span>${num}. ${escapeHtml(displayName)}</span>
          <span class="hz-vol" style="color: var(--neon-blue); font-family: var(--font-mono);">[ ${count} VOL ]</span>
        </div>
      </li>
    `;
  }).join('');
}

/* ════════════════════════════════════════════════════════════
   6. MAIN ROUTINE: FETCH CORE DATA
   ════════════════════════════════════════════════════════════ */

async function fetchActivity(background = false) {
  const url = new URL(ACTIVITY_URL, window.location.origin);

  // Build query params
  const source = document.getElementById('filter-source').value;
  const sentiment = document.getElementById('filter-sentiment').value;
  const topic = document.getElementById('filter-topic')?.value;
  const search = document.getElementById('filter-search')?.value;
  const timeRange = document.getElementById('filter-time')?.value;

  if (source) url.searchParams.set('source', source);
  if (topic) url.searchParams.set('topic', topic);
  if (search) url.searchParams.set('search', search);
  if (timeRange) url.searchParams.set('time_range', timeRange);

  url.searchParams.set('limit', '1000'); // Standard fetch limit aligned with DB limit

  const overlay = document.getElementById('loading-overlay');
  const dot = document.getElementById('status-dot');
  const indicator = document.getElementById('live-indicator');

  if (!background) {
    overlay.classList.remove('hidden');
    dot.style.backgroundColor = '#ffcc00'; // Yellow while loading
    dot.style.boxShadow = '0 0 8px #ffcc00';
    indicator.textContent = 'SCANNING...';
  }

  try {
    const res = await fetch(url.toString());
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const geojson = await res.json();
    allFeatures = geojson.features || [];

    // Filter and update UI
    applyFilters();
    updateLiveTicker(allFeatures);
    updateLiveInterceptor(allFeatures);
    updateHotZones();

    // Restore live dot
    dot.style.backgroundColor = 'var(--neon-green)';
    dot.style.boxShadow = '0 0 8px var(--neon-green)';
    indicator.textContent = 'LIVE STREAM';

    const ts = new Date().toLocaleTimeString('en-US', { hour12: false });
    document.getElementById('last-updated').textContent = `SYS.UPDATED: ${ts}`;

  } catch (err) {
    console.error('Datastream Error:', err);
    dot.style.backgroundColor = 'var(--neon-red)';
    dot.style.boxShadow = '0 0 8px var(--neon-red)';
    indicator.textContent = 'STREAM FAILED';
  } finally {
    if (!background) {
      setTimeout(() => overlay.classList.add('hidden'), 500); // small delay for visual effect
    }
  }
}

/* ════════════════════════════════════════════════════════════
   7. UI EVENT LISTENERS
   ════════════════════════════════════════════════════════════ */

// Map Layer toggle buttons (only visible in 2D Map view)
document.getElementById('layer-toggle')?.addEventListener('click', e => {
  const btn = e.target.closest('.toggle-btn');
  if (!btn) return;

  document.querySelectorAll('#layer-toggle .toggle-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');

  activeMapLayer = btn.dataset.layer;
  applyFilters();
});

// Primary Apply Filters button
document.getElementById('btn-apply').addEventListener('click', () => {
  fetchActivity();
  loadStats();
});

// Reset Filters button
document.getElementById('btn-reset').addEventListener('click', () => {
  document.getElementById('filter-source').value = '';
  document.getElementById('filter-sentiment').value = '';
  document.getElementById('filter-search').value = '';
  document.getElementById('filter-time').value = '';
  document.getElementById('filter-topic').value = '';

  fetchActivity();
  loadStats();
});

// Form inputs "Enter" key trigger
function handleEnter(e) {
  if (e.key === 'Enter') {
    fetchActivity();
    loadStats();
  }
}
document.getElementById('filter-search')?.addEventListener('keypress', handleEnter);

/* ════════════════════════════════════════════════════════════
   8. SYSTEM INITIALIZATION
   ════════════════════════════════════════════════════════════ */

(async () => {
  // 1. Init UI components
  initViewSwitcher();
  initUIPanels();
  initGlobe();

  // 2. Initial Data Pull
  await Promise.allSettled([
    fetchActivity(false),
    loadStats(),
  ]);

  // 3. Heartbeat (Background sync every 10s)
  setInterval(() => {
    fetchActivity(true);
    loadStats();
  }, 10000);
})();
