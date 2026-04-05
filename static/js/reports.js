/**
 * @file reports.js — Reports Dashboard visualisations
 *
 * Powers the D3-based reports dashboard for The Crawl Street Journal.
 * Each tab in the UI corresponds to a named entry in the `VIZ` registry
 * (e.g. VIZ.network, VIZ.treemap).  Tabs are lazy-rendered: the first
 * time a tab becomes active its VIZ function fires, fetches data from
 * the Flask viz_api.py endpoints (domains, graph, freshness, chord,
 * navigation, tags, technology, authorship, schema_insights,
 * filter_options), and draws into its panel's SVG container.
 *
 * Data flow:
 *   1. `window.ECO_API` (set in the HTML template) holds endpoint URLs.
 *   2. `fetchJSON()` wraps `d3.json()` with an in-memory `cache` map so
 *      repeat calls for the same URL avoid redundant network requests.
 *   3. The global filter system (bottom of file) monkey-patches
 *      `fetchJSON` to append filter query-string params automatically.
 *   4. When filters change, `cache` and `rendered` are cleared so every
 *      panel re-fetches with the new filter state on next activation.
 *
 * Key libraries: D3 v7, d3-cloud (word cloud layout).
 *
 * @see viz_api.py for the Flask endpoints that supply JSON data.
 */
/* ================================================================
   Ecosystem Dashboard — D3.js Visualisations
   ================================================================
   Lazy-loads data per tab. Each viz function receives its container
   selector and renders into it.  Shared helpers at the top.
   ================================================================ */
(function () {
  "use strict";

  /** Base URLs for every viz_api endpoint, injected by the Jinja template. */
  var API = window.ECO_API;

  /**
   * In-memory response cache keyed by full URL (including query string).
   * Cleared wholesale when the global filter bar changes so stale data
   * is never re-used after a filter toggle.
   * @type {Object.<string, *>}
   */
  var cache = {};

  /**
   * Tracks which panels have already been rendered so each VIZ function
   * runs at most once per filter cycle.  Cleared alongside `cache`.
   * @type {Object.<string, boolean>}
   */
  var rendered = {};

  // ── Colour palettes ─────────────────────────────────────────────

  /** Lazily-built map of ownership group name → hex colour. */
  var OWNER_COLOURS = {};
  var _ownerIndex = 0;

  /** HTTP status-code family colours: 2xx green, 3xx/4xx amber, 5xx red. */
  var STATUS_COLOURS = { "2": "#2D6A4F", "3": "#C4841D", "4": "#B8860B", "5": "#A4243B" };

  /** Shared 15-colour brand palette used across all charts as a fallback. */
  var CSJ_PALETTE = [
    "#1A1A1A", "#C4841D", "#2D6A4F", "#A4243B", "#5A5246",
    "#7B6D53", "#3A6B7E", "#8B5E3C", "#6B4226", "#4A6741",
    "#9B7042", "#B07D3A", "#3D5A4C", "#7A4F5A", "#5C7A6B"
  ];

  /**
   * Walk an array of domain objects and assign a stable colour to each
   * unique ownership group.  Colours cycle through CSJ_PALETTE so the
   * same group always gets the same colour within one page session.
   *
   * @param {Array.<{ownership: string}>} domains - Domain summary objects.
   */
  function _assignOwnerColours(domains) {
    domains.forEach(function (d) {
      var o = d.ownership;
      if (o && !OWNER_COLOURS[o]) {
        OWNER_COLOURS[o] = CSJ_PALETTE[_ownerIndex % CSJ_PALETTE.length];
        _ownerIndex++;
      }
    });
  }

  // ── Tooltip ─────────────────────────────────────────────────────

  // Single shared tooltip element reused by every chart — avoids creating
  // per-chart tooltip divs and keeps z-index management simple.
  var tip = d3.select("#vizTooltip");

  /**
   * Position and show the shared tooltip near the cursor.
   * Clamped horizontally so the tooltip never overflows the viewport.
   *
   * @param {MouseEvent} evt  - The triggering mouse event (used for pageX/Y).
   * @param {string}     html - Inner HTML content to display.
   */
  function showTip(evt, html) {
    tip.html(html).classed("visible", true);
    // Prevent the tooltip from being clipped by the right edge of the viewport.
    var tx = Math.min(evt.pageX + 14, window.innerWidth - 340);
    var ty = evt.pageY - 10;
    tip.style("left", tx + "px").style("top", ty + "px");
  }

  /** Hide the shared tooltip by toggling its CSS visibility class. */
  function hideTip() { tip.classed("visible", false); }

  // ── Helpers ─────────────────────────────────────────────────────

  /**
   * Format a number with locale-style comma grouping (e.g. 1,234).
   * @param {number} n
   * @returns {string}
   */
  function fmt(n) { return d3.format(",")(n); }

  /**
   * Strip leading "www." for display labels so domain names stay compact.
   * @param {string} d - Fully-qualified domain name.
   * @returns {string}
   */
  function shortDomain(d) { return d.replace(/^www\./, ""); }

  /**
   * Hide a loading spinner by adding the "hidden" CSS class.
   * @param {string} id - DOM id of the loading element.
   */
  function hideLoading(id) { var el = document.getElementById(id); if (el) el.classList.add("hidden"); }

  /**
   * Fetch JSON from a viz_api endpoint, returning a cached copy when
   * one exists.  This is the *original* implementation; the global
   * filter system later replaces it with a wrapper that appends filter
   * query-string params before delegating back here (see bottom of file).
   *
   * @param {string} url - Absolute or relative endpoint URL.
   * @returns {Promise<*>} Parsed JSON payload.
   */
  function fetchJSON(url) {
    if (cache[url]) return Promise.resolve(cache[url]);
    return d3.json(url).then(function (d) { cache[url] = d; return d; });
  }

  /**
   * Look up the colour assigned to an ownership group, falling back to
   * the muted taupe default when the group has not been seen before.
   *
   * @param {string} o - Ownership group name.
   * @returns {string} Hex colour.
   */
  function ownerColour(o) { return OWNER_COLOURS[o] || "#5A5246"; }

  /**
   * Populate a legend container with colour swatches and labels.
   *
   * @param {string} id    - DOM id of the legend wrapper element.
   * @param {Array.<{label: string, colour: string}>} items - Legend entries.
   */
  function buildLegend(id, items) {
    var el = document.getElementById(id);
    if (!el) return;
    el.innerHTML = items.map(function (it) {
      return '<span class="viz-legend-item"><span class="viz-legend-swatch" style="background:' +
        it.colour + ';"></span>' + it.label + '</span>';
    }).join("");
  }

  /**
   * Measure the available width of a chart container and derive a
   * height that keeps a roughly 16:10 aspect ratio, clamped between
   * 500 px and 700 px.
   *
   * @param {string} id - DOM id of the chart container.
   * @returns {{w: number, h: number}}
   */
  function vizSize(id) {
    var el = document.getElementById(id);
    var w = el ? el.clientWidth : 900;
    return { w: w, h: Math.max(500, Math.min(w * 0.65, 700)) };
  }

  // ── Tab controller ──────────────────────────────────────────────
  // Each `.viz-tab` button carries a `data-panel` attribute whose value
  // matches a key in the VIZ registry *and* corresponds to a panel id
  // of the form "panel-{name}".  Clicking a tab swaps the active class
  // on both the button row and the panel container, then calls
  // `renderPanel` to lazy-initialise the chart if it hasn't been drawn.

  var tabs = document.querySelectorAll(".viz-tab");
  var panels = document.querySelectorAll(".viz-panel");

  tabs.forEach(function (tab) {
    tab.addEventListener("click", function () {
      tabs.forEach(function (t) { t.classList.remove("active"); });
      panels.forEach(function (p) { p.classList.remove("active"); });
      tab.classList.add("active");
      var panelId = "panel-" + tab.dataset.panel;
      document.getElementById(panelId).classList.add("active");
      renderPanel(tab.dataset.panel);
    });
  });

  /**
   * Render a panel's chart if it has not already been drawn.  The
   * `rendered` guard ensures each chart is built only once per filter
   * cycle (cleared when global filters change).
   *
   * On every call that actually draws (i.e. `rendered[name]` was false),
   * the viz container is wiped and the loading spinner is restored so that
   * filter-driven re-renders don't stack a new SVG on top of the old one
   * or leave the panel blank while new data loads.
   *
   * @param {string} name - Panel key, must match a property on VIZ.
   */
  function renderPanel(name) {
    if (rendered[name]) return;
    rendered[name] = true;

    var vizEl = document.getElementById("viz-" + name);
    if (vizEl) vizEl.innerHTML = "";

    var loadingEl = document.getElementById("loading-" + name);
    if (loadingEl) loadingEl.classList.remove("hidden");

    var fn = VIZ[name];
    if (fn) fn();
  }

  // ── Viz implementations ─────────────────────────────────────────
  /** Registry of chart-builder functions, keyed by panel name. */
  var VIZ = {};

  // ────────────────────────────────────────────────────────────────
  // 1. Force-Directed Network Graph
  // ────────────────────────────────────────────────────────────────

  /**
   * Build an interactive force-directed network graph showing
   * cross-domain link relationships.  Fetches from both the
   * `domains` and `graph` endpoints in parallel.
   *
   * Features: zoom/pan, node-click selection with 2-hop
   * neighbourhood highlighting, detail overlay panel, and a
   * colour-by dropdown (ownership / CMS / extraction coverage).
   */
  VIZ.network = function () {
    // Fast lookup from domain string to its full summary object,
    // populated once the domains endpoint resolves.
    var domainLookup = {};

    Promise.all([fetchJSON(API.domains), fetchJSON(API.graph)]).then(function (results) {
      var domains = results[0];
      var data = results[1];

      _assignOwnerColours(domains);
      domains.forEach(function (d) { domainLookup[d.domain] = d; });

      hideLoading("loading-network");
      var sz = vizSize("viz-network");
      var W = sz.w, H = sz.h;

      // ── Adjacency map ────────────────────────────────────────
      // Pre-compute an adjacency set per node so the selection
      // logic can walk 1-hop and 2-hop neighbours in O(degree)
      // rather than scanning every link on each click.
      var adj = {};
      data.nodes.forEach(function (n) { adj[n.id] = new Set(); });
      data.links.forEach(function (l) {
        // After the simulation mutates link objects, source/target
        // become node objects with an `.id`; before that they are
        // plain strings.  Handle both forms.
        var s = l.source.id || l.source;
        var t = l.target.id || l.target;
        if (!adj[s]) adj[s] = new Set();
        if (!adj[t]) adj[t] = new Set();
        adj[s].add(t);
        adj[t].add(s);
      });

      var selectedNode = null;
      var overlay = document.getElementById("networkDetailOverlay");

      var svg = d3.select("#viz-network").append("svg")
        .attr("width", W).attr("height", H)
        .attr("viewBox", [0, 0, W, H]);

      // All visual elements live inside `g` so that the zoom transform
      // can be applied to a single group rather than individual shapes.
      var g = svg.append("g");

      // d3.zoom wired to the SVG; transforms the inner <g> so nodes,
      // links, and labels all pan/zoom together.
      svg.call(d3.zoom().scaleExtent([0.2, 5]).on("zoom", function (evt) {
        g.attr("transform", evt.transform);
      }));

      var maxPages = d3.max(data.nodes, function (n) { return n.pages; }) || 1;
      // Square-root scale so area grows linearly with page count.
      var rScale = d3.scaleSqrt().domain([0, maxPages]).range([3, 40]);
      var maxWeight = d3.max(data.links, function (l) { return l.weight; }) || 1;

      // Force simulation: link distance shrinks and strength grows with
      // link weight so heavily-connected nodes cluster closer together.
      var sim = d3.forceSimulation(data.nodes)
        .force("link", d3.forceLink(data.links).id(function (d) { return d.id; })
          .distance(function (d) { return 120 - Math.min(d.weight / maxWeight * 60, 50); })
          .strength(function (d) { return 0.2 + d.weight / maxWeight * 0.5; }))
        .force("charge", d3.forceManyBody().strength(-200))
        .force("center", d3.forceCenter(W / 2, H / 2))
        .force("collision", d3.forceCollide().radius(function (d) { return rScale(d.pages) + 2; }));

      // D3 enter-selection: one <line> per link, width proportional to weight.
      var link = g.selectAll("line").data(data.links).enter().append("line")
        .attr("stroke", "#ccc")
        .attr("stroke-width", function (d) { return Math.max(0.5, Math.min(d.weight / maxWeight * 4, 6)); })
        .attr("stroke-opacity", 0.4);

      // D3 enter-selection: one <circle> per node (domain).
      var node = g.selectAll("circle").data(data.nodes).enter().append("circle")
        .attr("r", function (d) { return rScale(d.pages); })
        .attr("fill", function (d) { return ownerColour(d.ownership); })
        .attr("stroke", "#fff")
        .attr("stroke-width", 1.5)
        .attr("cursor", "pointer")
        .on("mouseover", function (evt, d) {
          if (selectedNode && selectedNode.id === d.id) return;
          showTip(evt, "<strong>" + shortDomain(d.id) + "</strong><br>" +
            d.ownership + "<br>Pages: " + fmt(d.pages));
        })
        .on("mouseout", hideTip)
        .on("click", function (evt, d) {
          evt.stopPropagation();
          hideTip();
          selectNode(d);
        })
        .call(d3.drag()
          .on("start", function (evt, d) { if (!evt.active) sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
          .on("drag", function (evt, d) { d.fx = evt.x; d.fy = evt.y; })
          .on("end", function (evt, d) { if (!evt.active) sim.alphaTarget(0); d.fx = null; d.fy = null; })
        );

      // Only label nodes with >20 pages to avoid visual clutter.
      var labels = g.selectAll("text").data(data.nodes.filter(function (n) { return n.pages > 20; }))
        .enter().append("text")
        .text(function (d) { return shortDomain(d.id); })
        .attr("font-size", 10)
        .attr("fill", "#1A1A1A")
        .attr("text-anchor", "middle")
        .attr("dy", function (d) { return rScale(d.pages) + 12; })
        .attr("pointer-events", "none");

      // On every simulation tick, update the x/y positions of all
      // lines, circles, and text elements from the simulation state.
      sim.on("tick", function () {
        link.attr("x1", function (d) { return d.source.x; })
          .attr("y1", function (d) { return d.source.y; })
          .attr("x2", function (d) { return d.target.x; })
          .attr("y2", function (d) { return d.target.y; });
        node.attr("cx", function (d) { return d.x; })
          .attr("cy", function (d) { return d.y; });
        labels.attr("x", function (d) { return d.x; })
          .attr("y", function (d) { return d.y; });
      });

      // ── Selection logic ──────────────────────────────────────

      /**
       * Select a node and highlight its 2-hop neighbourhood.
       * "Primary" = directly connected (1 hop).
       * "Secondary" = 2 hops away (neighbours of neighbours).
       * Everything else fades to near-transparent.
       *
       * @param {Object} d - D3 datum for the clicked node.
       */
      function selectNode(d) {
        // Clicking the already-selected node toggles selection off.
        if (selectedNode && selectedNode.id === d.id) {
          clearSelection();
          return;
        }
        selectedNode = d;
        var primary = adj[d.id] || new Set();
        // Walk each primary neighbour's adjacency to build the 2-hop set.
        var secondary = new Set();
        primary.forEach(function (nid) {
          (adj[nid] || new Set()).forEach(function (nid2) {
            if (nid2 !== d.id && !primary.has(nid2)) secondary.add(nid2);
          });
        });

        node
          .attr("fill-opacity", function (n) {
            if (n.id === d.id) return 1;
            if (primary.has(n.id)) return 1;
            if (secondary.has(n.id)) return 0.6;
            return 0.08;
          })
          .attr("stroke", function (n) {
            if (n.id === d.id) return "#1A1A1A";
            if (primary.has(n.id)) return ownerColour(n.ownership);
            return "#fff";
          })
          .attr("stroke-width", function (n) {
            if (n.id === d.id) return 4;
            if (primary.has(n.id)) return 2.5;
            return 1.5;
          });

        link
          .attr("stroke", function (l) {
            var sid = l.source.id || l.source;
            var tid = l.target.id || l.target;
            var touchesSelected = sid === d.id || tid === d.id;
            if (touchesSelected) return "#1A1A1A";
            var touchesPrimary = primary.has(sid) || primary.has(tid);
            var bothPrimary = primary.has(sid) && primary.has(tid);
            var oneIsPrimaryOtherSecondary =
              (primary.has(sid) && secondary.has(tid)) ||
              (primary.has(tid) && secondary.has(sid));
            if (bothPrimary || oneIsPrimaryOtherSecondary) return "#5A5246";
            return "#ccc";
          })
          .attr("stroke-opacity", function (l) {
            var sid = l.source.id || l.source;
            var tid = l.target.id || l.target;
            if (sid === d.id || tid === d.id) return 0.85;
            if (primary.has(sid) && primary.has(tid)) return 0.35;
            if ((primary.has(sid) && secondary.has(tid)) ||
                (primary.has(tid) && secondary.has(sid))) return 0.25;
            return 0.04;
          })
          .attr("stroke-width", function (l) {
            var sid = l.source.id || l.source;
            var tid = l.target.id || l.target;
            var base = Math.max(0.5, Math.min(l.weight / maxWeight * 4, 6));
            if (sid === d.id || tid === d.id) return base * 1.8;
            return base;
          });

        labels.attr("fill-opacity", function (n) {
          if (n.id === d.id) return 1;
          if (primary.has(n.id)) return 1;
          if (secondary.has(n.id)) return 0.5;
          return 0.08;
        });

        populateOverlay(d, primary, secondary);
      }

      /** Reset all nodes, links, and labels to their default visual state. */
      function clearSelection() {
        selectedNode = null;
        node
          .attr("fill-opacity", 1)
          .attr("stroke", "#fff")
          .attr("stroke-width", 1.5);
        link
          .attr("stroke", "#ccc")
          .attr("stroke-opacity", 0.4)
          .attr("stroke-width", function (d) { return Math.max(0.5, Math.min(d.weight / maxWeight * 4, 6)); });
        labels.attr("fill-opacity", 1);
        overlay.classList.remove("open");
      }

      // Clicking empty SVG space or the overlay close button deselects.
      svg.on("click", function () { clearSelection(); });
      document.getElementById("ndo-close").addEventListener("click", function () { clearSelection(); });

      // ── Overlay population ───────────────────────────────────

      /**
       * Fill the side-panel overlay with stats, signals, and
       * connection lists for the selected domain node.
       *
       * @param {Object} d         - Selected node datum.
       * @param {Set}    primary   - Set of directly-connected node ids.
       * @param {Set}    secondary - Set of 2-hop node ids.
       */
      function populateOverlay(d, primary, secondary) {
        document.getElementById("ndo-domain").textContent = shortDomain(d.id);
        var ownerEl = document.getElementById("ndo-owner");
        ownerEl.textContent = d.ownership;
        ownerEl.style.background = ownerColour(d.ownership) + "18";
        ownerEl.style.color = ownerColour(d.ownership);

        var detail = domainLookup[d.id] || {};
        var stats = document.getElementById("ndo-stats");
        var statItems = [
          { val: fmt(d.pages), label: "Pages" },
          { val: fmt(primary.size), label: "Connections" },
          { val: detail.error_count !== undefined ? fmt(detail.error_count) : "–", label: "Errors" },
          { val: detail.avg_word_count ? fmt(detail.avg_word_count) : "–", label: "Avg words" },
          { val: detail.max_depth !== undefined ? detail.max_depth : "–", label: "Max depth" },
          { val: detail.total_assets ? fmt(detail.total_assets) : "–", label: "Assets" }
        ];
        stats.innerHTML = statItems.map(function (s) {
          return '<div class="ndo-stat"><div class="ndo-stat-val">' + s.val +
            '</div><div class="ndo-stat-label">' + s.label + '</div></div>';
        }).join("");

        var sigSec = document.getElementById("ndo-signals-section");
        var sigEl = document.getElementById("ndo-signals");
        if (detail.analytics_tools && detail.analytics_tools.length) {
          sigSec.style.display = "";
          var badges = detail.analytics_tools.map(function (t) {
            return '<span class="ndo-badge">' + t + '</span>';
          });
          if (detail.has_privacy_policy) badges.push('<span class="ndo-badge">Privacy policy</span>');
          if (detail.latest_date) badges.push('<span class="ndo-badge">Updated ' + detail.latest_date + '</span>');
          sigEl.innerHTML = badges.join("");
        } else {
          sigSec.style.display = "none";
        }

        var linksByNode = {};
        data.links.forEach(function (l) {
          var sid = l.source.id || l.source;
          var tid = l.target.id || l.target;
          if (sid === d.id) linksByNode[tid] = (linksByNode[tid] || 0) + l.weight;
          if (tid === d.id) linksByNode[sid] = (linksByNode[sid] || 0) + l.weight;
        });

        var primaryList = [];
        primary.forEach(function (nid) { primaryList.push({ id: nid, weight: linksByNode[nid] || 0 }); });
        primaryList.sort(function (a, b) { return b.weight - a.weight; });

        var priEl = document.getElementById("ndo-primary");
        if (primaryList.length) {
          priEl.innerHTML = primaryList.map(function (c) {
            var nd = data.nodes.find(function (n) { return n.id === c.id; });
            var own = nd ? nd.ownership : "";
            return '<li class="ndo-conn-primary">' +
              '<span class="ndo-conn-swatch" style="background:' + ownerColour(own) + ';"></span>' +
              shortDomain(c.id) +
              '<span class="ndo-conn-weight">' + fmt(c.weight) + ' links</span></li>';
          }).join("");
        } else {
          priEl.innerHTML = '<li style="color:var(--csj-body);font-style:italic;">No direct connections</li>';
        }

        var secSection = document.getElementById("ndo-secondary-section");
        var secEl = document.getElementById("ndo-secondary");
        if (secondary.size) {
          secSection.style.display = "";
          var secList = [];
          secondary.forEach(function (nid) { secList.push(nid); });
          secList.sort();
          secEl.innerHTML = secList.map(function (nid) {
            var nd = data.nodes.find(function (n) { return n.id === nid; });
            var own = nd ? nd.ownership : "";
            return '<li class="ndo-conn-secondary">' +
              '<span class="ndo-conn-swatch" style="background:' + ownerColour(own) + ';"></span>' +
              shortDomain(nid) + '</li>';
          }).join("");
        } else {
          secSection.style.display = "none";
        }

        overlay.classList.add("open");
      }

      // ── Colour-by toggle ─────────────────────────────────────
      // Pre-build CMS colour assignments so the dropdown can
      // switch colour modes without re-fetching data.
      var CMS_COLOURS = {};
      var _cmsIdx = 0;
      domains.forEach(function (d) {
        var c = d.cms_generator || "(undetected)";
        if (!CMS_COLOURS[c]) { CMS_COLOURS[c] = CSJ_PALETTE[_cmsIdx % CSJ_PALETTE.length]; _cmsIdx++; }
      });
      // Viridis gives a perceptually-uniform ramp for continuous coverage %.
      var covScale = d3.scaleSequential(d3.interpolateViridis).domain([0, 100]);

      /**
       * Determine fill colour for a node based on the current
       * colour-by dropdown value (ownership, CMS, or coverage).
       *
       * @param {Object} d - Node datum.
       * @returns {string} CSS colour string.
       */
      function nodeColour(d) {
        var mode = document.getElementById("network-colour-by").value;
        var detail = domainLookup[d.id] || {};
        if (mode === "cms") return CMS_COLOURS[detail.cms_generator || "(undetected)"] || "#5A5246";
        if (mode === "coverage") return covScale(detail.avg_extraction_coverage || 0);
        return ownerColour(d.ownership);
      }

      /** Re-apply node fills and rebuild the legend to match the active colour mode. */
      function recolourNodes() {
        node.attr("fill", nodeColour);
        var mode = document.getElementById("network-colour-by").value;
        if (mode === "cms") {
          buildLegend("legend-network", Object.keys(CMS_COLOURS).map(function (k) {
            return { label: k, colour: CMS_COLOURS[k] };
          }));
        } else if (mode === "coverage") {
          buildLegend("legend-network", [
            { label: "High coverage", colour: covScale(90) },
            { label: "Medium", colour: covScale(50) },
            { label: "Low coverage", colour: covScale(10) },
          ]);
        } else {
          buildLegend("legend-network", Object.keys(OWNER_COLOURS).map(function (k) {
            return { label: k, colour: OWNER_COLOURS[k] };
          }));
        }
      }

      document.getElementById("network-colour-by").addEventListener("change", recolourNodes);

      buildLegend("legend-network", Object.keys(OWNER_COLOURS).map(function (k) {
        return { label: k, colour: OWNER_COLOURS[k] };
      }));
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 2. Zoomable Treemap
  // ────────────────────────────────────────────────────────────────

  /**
   * Render a treemap where rectangle area is proportional to page count.
   * The user can switch between grouping by ownership, CMS, or primary
   * content kind via a dropdown.  Fully redrawn on group-by change
   * because d3.treemap layout depends on the hierarchy shape.
   */
  VIZ.treemap = function () {
    fetchJSON(API.domains).then(function (data) {
      hideLoading("loading-treemap");
      _assignOwnerColours(data);

      // Treemap-local colour map; reset on each redraw so the
      // palette cycles from index 0 every time the grouping changes.
      var TM_COLOURS = {};
      var _tmIdx = 0;
      /** @param {string} key - Group name. @returns {string} */
      function tmColour(key) {
        if (!TM_COLOURS[key]) { TM_COLOURS[key] = CSJ_PALETTE[_tmIdx % CSJ_PALETTE.length]; _tmIdx++; }
        return TM_COLOURS[key];
      }

      /** Tear down the existing treemap SVG and rebuild from scratch. */
      function drawTreemap() {
        d3.select("#viz-treemap").selectAll("*").remove();
        TM_COLOURS = {}; _tmIdx = 0;
        var mode = document.getElementById("treemap-group-by").value;
        var sz = vizSize("viz-treemap");
        var W = sz.w, H = sz.h;

        var grouped = {};
        data.forEach(function (d) {
          var key;
          if (mode === "cms") key = d.cms_generator || "(undetected)";
          else if (mode === "content") {
            var kinds = d.content_kinds || {};
            var topKind = Object.keys(kinds).sort(function (a, b) { return kinds[b] - kinds[a]; })[0] || "(unclassified)";
            key = topKind;
          } else {
            key = d.ownership;
          }
          if (!grouped[key]) grouped[key] = [];
          grouped[key].push(d);
        });

        var colourFn = mode === "ownership" ? ownerColour : tmColour;

        // Build a two-level hierarchy: root → group → leaf (domain).
        var root = d3.hierarchy({
          name: "estate",
          children: Object.keys(grouped).map(function (k) {
            return {
              name: k,
              children: grouped[k].map(function (d) {
                return { name: d.domain, value: d.page_count, data: d };
              })
            };
          })
        }).sum(function (d) { return d.value || 0; })
          .sort(function (a, b) { return b.value - a.value; });

        // paddingTop reserves space for the group label rendered above each cluster.
        d3.treemap().size([W, H]).padding(2).paddingTop(18).round(true)(root);
        var svg = d3.select("#viz-treemap").append("svg").attr("width", W).attr("height", H);

        // All visual elements live inside `g` so the zoom transform targets
        // a single group rather than individual shapes.
        var g = svg.append("g");

        // Full scroll/drag/pinch zoom — no click-drill to conflict with.
        svg.call(d3.zoom()
          .scaleExtent([0.5, 8])
          .on("zoom", function (evt) { g.attr("transform", evt.transform); })
        );

        // Group-level rects (outlines) and labels drawn first so leaf
        // rects sit on top in z-order.
        var groups = g.selectAll("g").data(root.children).enter().append("g");
        groups.append("rect")
          .attr("x", function (d) { return d.x0; }).attr("y", function (d) { return d.y0; })
          .attr("width", function (d) { return d.x1 - d.x0; })
          .attr("height", function (d) { return d.y1 - d.y0; })
          .attr("fill", "none").attr("stroke", function (d) { return colourFn(d.data.name); })
          .attr("stroke-width", 2);
        groups.append("text")
          .attr("x", function (d) { return d.x0 + 4; }).attr("y", function (d) { return d.y0 + 13; })
          .text(function (d) { return d.data.name + " (" + fmt(d.value) + " pages)"; })
          .attr("font-size", 11).attr("font-weight", 700)
          .attr("fill", function (d) { return colourFn(d.data.name); });

        var leaves = g.selectAll(".leaf").data(root.leaves()).enter().append("g").attr("class", "leaf");
        leaves.append("rect")
          .attr("x", function (d) { return d.x0; }).attr("y", function (d) { return d.y0; })
          .attr("width", function (d) { return d.x1 - d.x0; })
          .attr("height", function (d) { return d.y1 - d.y0; })
          .attr("fill", function (d) { return colourFn(d.parent.data.name); })
          .attr("fill-opacity", 0.7).attr("stroke", "#fff").attr("stroke-width", 0.5)
          .on("mouseover", function (evt, d) {
            d3.select(this).attr("fill-opacity", 1);
            var info = d.data.data || {};
            showTip(evt, "<strong>" + shortDomain(d.data.name) + "</strong><br>" +
              "Pages: " + fmt(d.data.value) +
              (info.cms_generator ? "<br>CMS: " + info.cms_generator : "") +
              (info.total_assets ? "<br>Assets: " + fmt(info.total_assets) : "") +
              (info.avg_word_count ? "<br>Avg words: " + fmt(info.avg_word_count) : ""));
          })
          .on("mouseout", function () { d3.select(this).attr("fill-opacity", 0.7); hideTip(); });
        leaves.append("text")
          .attr("x", function (d) { return d.x0 + 3; }).attr("y", function (d) { return d.y0 + 13; })
          .text(function (d) { return (d.x1 - d.x0 > 50 && d.y1 - d.y0 > 16) ? shortDomain(d.data.name) : ""; })
          .attr("font-size", 10).attr("fill", "#fff").attr("pointer-events", "none");
      }

      drawTreemap();
      document.getElementById("treemap-group-by").addEventListener("change", drawTreemap);
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 3. Stacked Bar Chart — Status & Errors
  // ────────────────────────────────────────────────────────────────

  /**
   * Horizontal stacked bar chart showing the HTTP status-code
   * distribution per domain.  Each bar segment represents a status
   * family (2xx, 3xx, 4xx, 5xx, unknown).  Re-sorts and redraws
   * fully when the sort dropdown changes because bar layout is
   * computed imperatively (no D3 general-update pattern).
   */
  VIZ.status = function () {
    fetchJSON(API.domains).then(function (data) {
      hideLoading("loading-status");
      var top = data.slice(0, 40);
      var margin = { top: 10, right: 20, bottom: 10, left: 200 };
      var barH = 20, gap = 3;
      var categories = ["2", "3", "4", "5", "?"];
      var catColours = { "2": "#2D6A4F", "3": "#C4841D", "4": "#B8860B", "5": "#A4243B", "?": "#5A5246" };

      /**
       * Sort the top-40 domain slice in place according to the chosen mode.
       * @param {string} mode - "errors" | "pages" | "alpha".
       */
      function sortData(mode) {
        if (mode === "errors") {
          // Error rate = error_count / page_count; highest rate first.
          top.sort(function (a, b) {
            var ae = (a.error_count || 0) / (a.page_count || 1);
            var be = (b.error_count || 0) / (b.page_count || 1);
            return be - ae;
          });
        } else if (mode === "pages") {
          top.sort(function (a, b) { return b.page_count - a.page_count; });
        } else {
          top.sort(function (a, b) { return a.domain.localeCompare(b.domain); });
        }
      }

      /** Tear down and redraw bars using the currently selected sort order. */
      function drawStatus() {
        d3.select("#viz-status").selectAll("*").remove();
        sortData(document.getElementById("status-sort").value || "errors");

        var sz = vizSize("viz-status");
        var W = sz.w;
        var innerW = W - margin.left - margin.right;
        var H = margin.top + margin.bottom + top.length * (barH + gap);

        var svg = d3.select("#viz-status").append("svg").attr("width", W).attr("height", H);
        var g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        var maxPages = d3.max(top, function (d) { return d.page_count; }) || 1;
        var x = d3.scaleLinear().domain([0, maxPages]).range([0, innerW]);

        // Build bars imperatively — one row per domain, segments stacked
        // left-to-right by status family.
        top.forEach(function (d, i) {
          var y = i * (barH + gap);
          g.append("text")
            .attr("x", -4).attr("y", y + barH / 2 + 4)
            .attr("text-anchor", "end").attr("font-size", 11).attr("fill", "#1A1A1A")
            .text(shortDomain(d.domain));

          var cumX = 0;
          var sc = d.status_codes || {};
          categories.forEach(function (cat) {
            // Aggregate all HTTP codes whose first digit matches this
            // category; "?" catches anything that doesn't start 1-5.
            var count = 0;
            Object.keys(sc).forEach(function (code) {
              if (code.charAt(0) === cat || (cat === "?" && !("12345".includes(code.charAt(0))))) count += sc[code];
            });
            if (count > 0) {
              var w = x(count);
              g.append("rect")
                .attr("x", cumX).attr("y", y).attr("width", w).attr("height", barH)
                .attr("fill", catColours[cat])
                .attr("rx", 1)
                .on("mouseover", function (evt) {
                  showTip(evt, "<strong>" + shortDomain(d.domain) + "</strong><br>" +
                    cat + "xx: " + fmt(count) + " of " + fmt(d.page_count) + " pages" +
                    "<br>Errors: " + fmt(d.error_count));
                })
                .on("mouseout", hideTip);
              cumX += w;
            }
          });
        });
      }

      drawStatus();
      document.getElementById("status-sort").addEventListener("change", drawStatus);
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 4. Analytics & Governance Matrix Heatmap
  // ────────────────────────────────────────────────────────────────

  /**
   * Dot strip chart of analytics/governance signal adoption.
   * Each row = one signal; each dot = one domain placed at its coverage %.
   * Dots are jittered vertically so overlapping values stay readable.
   */
  VIZ.analytics = function () {
    fetchJSON(API.domains).then(function (data) {
      hideLoading("loading-analytics");

      var signalSet = new Set();
      data.forEach(function (d) {
        (d.analytics_tools || []).forEach(function (s) { signalSet.add(s); });
      });
      var signals = Array.from(signalSet).sort();
      signals.push("privacy_policy");

      var top = data.filter(function (d) { return d.page_count >= 2; });

      if (!top.length || !signals.length) {
        d3.select("#viz-analytics").append("p")
          .attr("class", "viz-desc")
          .style("text-align", "center").style("padding", "60px 0")
          .text("No analytics or governance signals detected for domains with 2+ pages.");
        return;
      }

      top.forEach(function (d) {
        d._signalCount = (d.analytics_tools || []).length + (d.has_privacy_policy ? 1 : 0);
      });
      top.sort(function (a, b) { return b._signalCount - a._signalCount || b.page_count - a.page_count; });
      top = top.slice(0, 50);

      function signalLabel(sig) {
        if (sig === "privacy_policy") return "Privacy Policy";
        return sig.replace(/_/g, " ").replace(/\b\w/g, function (c) { return c.toUpperCase(); });
      }

      function coverage(d, sig) {
        if (!d.page_count) return 0;
        if (sig === "privacy_policy") return (d.privacy_policy_pages || 0) / d.page_count;
        return ((d.analytics_tool_pages || {})[sig] || 0) / d.page_count;
      }

      function isPresent(d, sig) {
        if (sig === "privacy_policy") return !!d.has_privacy_policy;
        return (d.analytics_tools || []).indexOf(sig) !== -1;
      }

      var sz = vizSize("viz-analytics");
      var labelW = 200, countW = 90, rowH = 52;
      var margin = { top: 16, right: countW + 16, bottom: 44, left: labelW };
      var W = sz.w, iW = W - margin.left - margin.right;
      var H = margin.top + signals.length * rowH + margin.bottom;

      var svg = d3.select("#viz-analytics").append("svg").attr("width", W).attr("height", H);
      var g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      var x = d3.scaleLinear().domain([0, 1]).range([0, iW]);

      [0, 0.25, 0.5, 0.75, 1].forEach(function (pct) {
        g.append("line")
          .attr("x1", x(pct)).attr("x2", x(pct))
          .attr("y1", 0).attr("y2", signals.length * rowH)
          .attr("stroke", "#E0D8CC").attr("stroke-width", 1)
          .attr("stroke-dasharray", pct === 0 || pct === 1 ? "none" : "3,3");
      });

      g.append("g").attr("transform", "translate(0," + signals.length * rowH + ")")
        .call(d3.axisBottom(x).tickValues([0, 0.25, 0.5, 0.75, 1]).tickFormat(function (v) { return Math.round(v * 100) + "%"; }))
        .call(function (a) { a.select(".domain").attr("stroke", "#C8C0B4"); })
        .call(function (a) { a.selectAll(".tick line").remove(); })
        .call(function (a) { a.selectAll("text").attr("font-size", 11).attr("fill", "#5A5246"); });

      var jitter = [-16, -8, 0, 8, 16];

      signals.forEach(function (sig, i) {
        var y0 = i * rowH, yMid = y0 + rowH / 2;
        var isPrivacy = sig === "privacy_policy";

        g.append("rect")
          .attr("x", 0).attr("y", y0).attr("width", iW).attr("height", rowH)
          .attr("fill", i % 2 === 0 ? "rgba(248,244,239,0.6)" : "rgba(255,255,255,0.4)");

        g.append("line")
          .attr("x1", 0).attr("x2", iW).attr("y1", y0).attr("y2", y0)
          .attr("stroke", "#E0D8CC").attr("stroke-width", 1);

        svg.append("text")
          .attr("x", margin.left - 10).attr("y", margin.top + yMid)
          .attr("text-anchor", "end").attr("dominant-baseline", "central")
          .attr("font-size", 12).attr("font-weight", 500)
          .attr("fill", isPrivacy ? "#2D6A4F" : "#1A1A1A")
          .text(signalLabel(sig));

        var dotsData = top.filter(function (d) { return isPresent(d, sig); });
        dotsData.sort(function (a, b) { return coverage(a, sig) - coverage(b, sig); });

        dotsData.forEach(function (d, di) {
          var cov = coverage(d, sig);
          var yOff = jitter[di % jitter.length];
          g.append("circle")
            .attr("cx", x(cov)).attr("cy", yMid + yOff).attr("r", 5)
            .attr("fill", isPrivacy ? "#2D6A4F" : "#3B7DB5")
            .attr("fill-opacity", 0.72)
            .attr("stroke", isPrivacy ? "#1A4030" : "#2B5D90").attr("stroke-width", 0.8)
            .on("mouseover", function (evt) {
              showTip(evt, "<strong>" + shortDomain(d.domain) + "</strong><br>" +
                signalLabel(sig) + ": " + Math.round(cov * 100) + "% of " + fmt(d.page_count) + " pages");
            })
            .on("mouseout", hideTip);
        });

        var adoptedCount = dotsData.length;
        svg.append("text")
          .attr("x", margin.left + iW + 10).attr("y", margin.top + yMid)
          .attr("dominant-baseline", "central").attr("font-size", 11).attr("fill", "#5A5246")
          .text(adoptedCount + "\u202f/\u202f" + top.length + " domains");
      });

      buildLegend("legend-analytics", [
        { colour: "#2D6A4F", label: "Privacy policy detected" },
        { colour: "#3B7DB5", label: "Analytics tool detected" },
        { colour: "#E0D8CC", label: "Not detected (absent from chart)" }
      ]);
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 5. Freshness Timeline
  // ────────────────────────────────────────────────────────────────

  /**
   * Horizontal Gantt-style timeline showing the date range (oldest →
   * latest) per domain or grouped by CMS / content kind.  Bar colour
   * is a Red-Yellow-Green ramp driven by days since the most recent
   * update (green = fresh, red = stale, 730-day window).
   */
  VIZ.freshness = function () {
    Promise.all([fetchJSON(API.freshness), fetchJSON(API.domains)]).then(function (results) {
      var data = results[0];
      var domainData = results[1];
      hideLoading("loading-freshness");

      /** Full teardown + redraw required when the group-by dropdown changes. */
      function drawFreshness() {
        d3.select("#viz-freshness").selectAll("*").remove();
        var mode = document.getElementById("freshness-group-by").value;
        var rows;

        if (mode === "domain") {
          rows = data.domains.slice(0, 50);
        } else {
          var groups = {};
          domainData.forEach(function (d) {
            var key;
            if (mode === "cms") key = d.cms_generator || "(undetected)";
            else {
              var kinds = d.content_kinds || {};
              key = Object.keys(kinds).sort(function (a, b) { return kinds[b] - kinds[a]; })[0] || "(unclassified)";
            }
            if (!groups[key]) groups[key] = { label: key, oldest: null, latest: null };
            var g = groups[key];
            if (d.oldest_date && (!g.oldest || d.oldest_date < g.oldest)) g.oldest = d.oldest_date;
            if (d.latest_date && (!g.latest || d.latest_date > g.latest)) g.latest = d.latest_date;
          });
          rows = Object.values(groups).filter(function (g) { return g.oldest || g.latest; })
            .map(function (g) { return { domain: g.label, oldest: g.oldest, latest: g.latest }; })
            .sort(function (a, b) { return (b.latest || "") > (a.latest || "") ? 1 : -1; })
            .slice(0, 50);
        }
        if (!rows.length) return;

        var margin = { top: 30, right: 30, bottom: 30, left: 200 };
        var barH = 16, gap = 4;
        var sz = vizSize("viz-freshness");
        var W = sz.w;
        var innerW = W - margin.left - margin.right;
        var H = margin.top + margin.bottom + rows.length * (barH + gap);

        var allDates = [];
        rows.forEach(function (d) {
          if (d.oldest) allDates.push(new Date(d.oldest));
          if (d.latest) allDates.push(new Date(d.latest));
        });
        var xMin = d3.min(allDates) || new Date("2020-01-01");
        var xMax = new Date(data.today);
        var x = d3.scaleTime().domain([xMin, xMax]).range([0, innerW]);
        // RdYlGn diverging ramp: 0 (stale) → red, 1 (fresh) → green.
        var freshColour = d3.scaleSequential(d3.interpolateRdYlGn).domain([0, 1]);

        var svg = d3.select("#viz-freshness").append("svg").attr("width", W).attr("height", H);
        var gEl = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");
        gEl.append("g").attr("transform", "translate(0,-8)")
          .call(d3.axisTop(x).ticks(8).tickFormat(d3.timeFormat("%b %Y")))
          .selectAll("text").attr("font-size", 10);

        var today = new Date(data.today);
        rows.forEach(function (d, i) {
          var y = i * (barH + gap);
          var label = mode === "domain" ? shortDomain(d.domain) : d.domain;
          svg.append("text")
            .attr("x", margin.left - 6).attr("y", margin.top + y + barH / 2 + 3)
            .attr("text-anchor", "end").attr("font-size", 10).attr("fill", "#1A1A1A")
            .text(label.length > 30 ? label.slice(0, 28) + "\u2026" : label);

          var oldest = d.oldest ? new Date(d.oldest) : xMin;
          var latest = d.latest ? new Date(d.latest) : xMin;
          var daysSinceUpdate = (today - latest) / (1000 * 60 * 60 * 24);
          var freshness = Math.max(0, Math.min(1, 1 - daysSinceUpdate / 730));

          gEl.append("rect")
            .attr("x", x(oldest)).attr("y", y)
            .attr("width", Math.max(2, x(latest) - x(oldest)))
            .attr("height", barH)
            .attr("fill", freshColour(freshness)).attr("rx", 2)
            .on("mouseover", function (evt) {
              showTip(evt, "<strong>" + label + "</strong><br>" +
                "Oldest: " + d.oldest + "<br>Latest: " + d.latest +
                "<br>Days since update: " + Math.round(daysSinceUpdate));
            })
            .on("mouseout", hideTip);
        });
      }

      drawFreshness();
      document.getElementById("freshness-group-by").addEventListener("change", drawFreshness);
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 6. Chord Diagram
  // ────────────────────────────────────────────────────────────────

  /**
   * Circular chord diagram of inter-domain linking.  The `chord`
   * endpoint returns a square matrix and a domain list; d3.chord
   * converts this into arc + ribbon geometry.  The "top N" dropdown
   * controls how many domains appear; changing it invalidates the
   * cache entry for that URL so a fresh matrix is fetched.
   */
  VIZ.chord = function () {
    var topN = document.getElementById("chord-top").value || 20;
    fetchJSON(API.chord + "?top=" + topN).then(function (data) {
      hideLoading("loading-chord");
      var sz = vizSize("viz-chord");
      var W = Math.min(sz.w, 700), H = W;
      var outerR = W / 2 - 80, innerR = outerR - 20;

      d3.select("#viz-chord").selectAll("*").remove();
      var outerSvg = d3.select("#viz-chord").append("svg").attr("width", W).attr("height", H);

      // `zoomG` receives the d3.zoom transform; `svg` is the chord's own
      // centring group nested inside it so the two transforms compose cleanly.
      var zoomG = outerSvg.append("g");
      var svg = zoomG.append("g").attr("transform", "translate(" + W / 2 + "," + H / 2 + ")");

      // Full scroll / drag / pinch zoom — the chord has no click handlers.
      outerSvg.call(d3.zoom()
        .scaleExtent([0.5, 5])
        .on("zoom", function (evt) { zoomG.attr("transform", evt.transform); })
      );

      var colour = d3.scaleOrdinal(CSJ_PALETTE);
      var chord = d3.chord().padAngle(0.04).sortSubgroups(d3.descending);
      var chords = chord(data.matrix);

      var arc = d3.arc().innerRadius(innerR).outerRadius(outerR);
      var ribbon = d3.ribbon().radius(innerR);

      svg.selectAll(".arc").data(chords.groups).enter().append("path")
        .attr("class", "arc")
        .attr("d", arc)
        .attr("fill", function (d) { return colour(d.index); })
        .attr("stroke", "#fff")
        .on("mouseover", function (evt, d) {
          showTip(evt, "<strong>" + shortDomain(data.domains[d.index]) + "</strong><br>Total links: " + fmt(d.value));
        })
        .on("mouseout", hideTip);

      svg.selectAll(".arc-label").data(chords.groups).enter().append("text")
        .each(function (d) { d.angle = (d.startAngle + d.endAngle) / 2; })
        .attr("dy", ".35em")
        .attr("transform", function (d) {
          return "rotate(" + (d.angle * 180 / Math.PI - 90) + ")" +
            "translate(" + (outerR + 8) + ")" +
            (d.angle > Math.PI ? "rotate(180)" : "");
        })
        .attr("text-anchor", function (d) { return d.angle > Math.PI ? "end" : null; })
        .attr("font-size", 9).attr("fill", "#1A1A1A")
        .text(function (d) { return shortDomain(data.domains[d.index]); });

      svg.selectAll(".ribbon").data(chords).enter().append("path")
        .attr("class", "ribbon")
        .attr("d", ribbon)
        .attr("fill", function (d) { return colour(d.source.index); })
        .attr("fill-opacity", 0.5)
        .attr("stroke", "none")
        .on("mouseover", function (evt, d) {
          showTip(evt,
            "<strong>" + shortDomain(data.domains[d.source.index]) + "</strong> → " +
            shortDomain(data.domains[d.target.index]) + ": " + fmt(d.source.value) + " links<br>" +
            shortDomain(data.domains[d.target.index]) + " → " +
            shortDomain(data.domains[d.source.index]) + ": " + fmt(d.target.value) + " links");
        })
        .on("mouseout", hideTip);
    });

  };

  // ────────────────────────────────────────────────────────────────
  // 7. Sunburst — Navigation Structure  (zoomable)
  // ────────────────────────────────────────────────────────────────

  /**
   * Zoomable sunburst that visualises the navigation tree for a single
   * domain.  The domain selector is populated from the `navigation`
   * endpoint; choosing a domain fetches its tree and calls
   * `renderSunburst`.  Zooming is animated via `d3.interpolate` on
   * the arc start/end angles and radii, storing interpolation targets
   * in `d._target` and the current state in `d._current`.
   */
  VIZ.sunburst = function () {
    fetchJSON(API.navigation).then(function (data) {
      hideLoading("loading-sunburst");
      var sel = document.getElementById("sunburst-domain");
      (data.domains || []).forEach(function (d) {
        var opt = document.createElement("option");
        opt.value = d.domain || d;
        opt.text = shortDomain(d.domain || d);
        sel.appendChild(opt);
      });

      sel.addEventListener("change", function () {
        if (!this.value) return;
        fetchJSON(API.navigation + "?domain=" + encodeURIComponent(this.value)).then(function (navData) {
          renderSunburst(navData.tree);
        });
      });

      if (data.domains && data.domains.length) {
        sel.value = data.domains[0].domain || data.domains[0];
        sel.dispatchEvent(new Event("change"));
      }
    });

    /* ---- colour helpers ---- */
    var SB_INTERNAL_HUE = 30;    // warm ink/amber family
    var SB_EXTERNAL_HUE = 160;   // muted teal family

    /**
     * Derive an HSL colour for a sunburst arc.  Internal and external
     * links start from different base hues; sibling index drives a
     * hue shift so adjacent segments are visually distinct.  Lightness
     * increases with depth so deeper levels feel "further away".
     *
     * @param {d3.HierarchyNode} d
     * @returns {string} CSS HSL colour.
     */
    function sbColour(d) {
      if (!d.parent) return "#F2EFEB";
      var isExt = d.data.external || (d.parent && d.parent.data.external);
      var baseHue = isExt ? SB_EXTERNAL_HUE : SB_INTERNAL_HUE;

      // Walk ancestors to accumulate a hue offset based on each node's
      // position among its siblings — deeper levels contribute smaller
      // shifts to keep the overall hue band coherent.
      var ancestors = d.ancestors().reverse();
      var hueShift = 0;
      for (var i = 1; i < ancestors.length; i++) {
        var sibs = ancestors[i].parent.children || [];
        var idx = sibs.indexOf(ancestors[i]);
        var spread = 40 / i;
        hueShift += (idx - sibs.length / 2) * (spread / Math.max(sibs.length, 1));
      }
      var hue = (baseHue + hueShift + 360) % 360;
      var sat = d.data.group ? 55 : 65;
      var light = Math.min(80, 38 + d.depth * 10);
      return "hsl(" + hue + "," + sat + "%," + light + "%)";
    }

    /* ---- breadcrumbs ---- */
    var breadcrumbEl = document.getElementById("sunburst-breadcrumbs");

    /**
     * Rebuild the breadcrumb trail for the current zoom target.
     * Each ancestor becomes a clickable span; the last item is
     * styled as "current" and is not interactive.
     *
     * @param {d3.HierarchyNode} node          - Current zoom target.
     * @param {Function}         clickHandler   - Called with a node to zoom into.
     */
    function updateBreadcrumbs(node, clickHandler) {
      if (!breadcrumbEl) return;
      var chain = node.ancestors().reverse();
      breadcrumbEl.innerHTML = "";
      chain.forEach(function (n, i) {
        if (i > 0) {
          var sep = document.createElement("span");
          sep.className = "sb-crumb-sep";
          sep.textContent = "›";
          breadcrumbEl.appendChild(sep);
        }
        var btn = document.createElement("span");
        btn.className = "sb-crumb" + (i === chain.length - 1 ? " sb-crumb-current" : "");
        btn.textContent = i === 0 ? shortDomain(n.data.name) : n.data.name;
        if (i < chain.length - 1) {
          (function (target) {
            btn.addEventListener("click", function () { clickHandler(target); });
          })(n);
        }
        breadcrumbEl.appendChild(btn);
      });
    }

    /* ---- arc label helper ---- */

    /**
     * Determine whether the arc segment is large enough to display a text label.
     * @param {Object} d - Node with partition layout coords (x0, x1, y0, y1).
     * @returns {boolean}
     */
    function labelFits(d) {
      return (d.y1 - d.y0) > 28 && (d.x1 - d.x0) > 0.06;
    }

    /**
     * Compute an SVG transform string that places a text label at the
     * midpoint of a partition arc, rotated to follow the arc's angle.
     *
     * @param {Object} d - Node with partition coords.
     * @param {number} R - Outer radius of the sunburst.
     * @returns {string} SVG transform.
     */
    function labelTransform(d, R) {
      var x = ((d.x0 + d.x1) / 2) * 180 / Math.PI;
      var y = (d.y0 + d.y1) / 2;
      return "rotate(" + (x - 90) + ") translate(" + y + ",0) rotate(" + (x < 180 ? 0 : 180) + ")";
    }

    /**
     * Draw (or redraw) the sunburst for a given navigation tree.
     * @param {Object} tree - Hierarchical tree returned by the navigation endpoint.
     */
    function renderSunburst(tree) {
      if (!tree) return;
      d3.select("#viz-sunburst").selectAll("svg").remove();
      if (breadcrumbEl) breadcrumbEl.innerHTML = "";

      var sz = vizSize("viz-sunburst");
      var W = Math.min(sz.w, 720), H = W;
      var R = W / 2;

      var root = d3.hierarchy(tree)
        .sum(function (d) { return d.children ? 0 : (d.size || 1); })
        .sort(function (a, b) { return b.value - a.value; });

      var partition = d3.partition().size([2 * Math.PI, R]);
      partition(root);

      var arc = d3.arc()
        .startAngle(function (d) { return d.x0; })
        .endAngle(function (d) { return d.x1; })
        .padAngle(function (d) { return Math.min((d.x1 - d.x0) / 2, 0.005); })
        .padRadius(R / 2)
        .innerRadius(function (d) { return d.y0; })
        .outerRadius(function (d) { return d.y1 - 1; });

      var svg = d3.select("#viz-sunburst").append("svg")
        .attr("width", W).attr("height", H)
        .append("g").attr("transform", "translate(" + R + "," + R + ")");

      var currentRoot = root;

      /* Centre circle acts as a "zoom out" button — clicking it
         navigates one level up in the hierarchy. */
      var centreCircle = svg.append("circle")
        .attr("r", root.y1 ? root.y1 : R * 0.18)
        .attr("fill", "#F2EFEB")
        .attr("cursor", "pointer")
        .on("click", function () { zoomTo(currentRoot.parent || root); });

      /* Centre text group */
      var centreG = svg.append("g")
        .attr("text-anchor", "middle")
        .attr("pointer-events", "none");
      var centreTitle = centreG.append("text")
        .attr("dy", "-0.3em")
        .attr("font-size", 14).attr("font-weight", 700).attr("fill", "#1A1A1A");
      var centreSubtitle = centreG.append("text")
        .attr("dy", "1.1em")
        .attr("font-size", 11).attr("fill", "#4A4A4A");

      function setCentreText(title, subtitle) {
        centreTitle.text(title);
        centreSubtitle.text(subtitle || "");
      }
      setCentreText(shortDomain(tree.name), root.value + " links");

      /* Arcs */
      var paths = svg.selectAll("path.sb-arc")
        .data(root.descendants().filter(function (d) { return d.depth; }))
        .enter().append("path")
        .attr("class", "sb-arc")
        .attr("d", arc)
        .attr("fill", sbColour)
        .attr("stroke", "#fff")
        .attr("stroke-width", 0.5)
        .attr("cursor", "pointer")
        .on("mouseover", function (evt, d) {
          d3.select(this).attr("stroke-width", 2).attr("stroke", "#1A1A1A");
          var parts = ["<strong>" + d.data.name + "</strong>"];
          if (d.data.href) parts.push(d.data.href);
          if (d.data.external) parts.push("<em>External link</em>");
          if (d.children) parts.push(d.value + " items");
          showTip(evt, parts.join("<br>"));
          setCentreText(d.data.name, d.children ? d.value + " items" : (d.data.href || ""));
        })
        .on("mouseout", function () {
          d3.select(this).attr("stroke-width", 0.5).attr("stroke", "#fff");
          hideTip();
          var cr = currentRoot;
          setCentreText(
            cr.depth === 0 ? shortDomain(tree.name) : cr.data.name,
            cr.value + " items"
          );
        })
        .on("click", function (evt, d) {
          if (d.children) zoomTo(d);
        });

      /* Arc labels */
      var labels = svg.selectAll("text.sb-label")
        .data(root.descendants().filter(function (d) { return d.depth; }))
        .enter().append("text")
        .attr("class", "sb-label")
        .attr("transform", function (d) { return labelTransform(d, R); })
        .attr("text-anchor", "middle")
        .attr("dy", "0.35em")
        .attr("font-size", 10)
        .attr("fill", function (d) {
          var l = d3.hsl(sbColour(d)).l;
          return l > 0.58 ? "#1A1A1A" : "#fff";
        })
        .attr("pointer-events", "none")
        .text(function (d) {
          if (!labelFits(d)) return "";
          var name = d.data.name;
          var maxLen = Math.floor((d.y1 - d.y0) / 6);
          return name.length > maxLen ? name.slice(0, maxLen - 1) + "…" : name;
        });

      // Raise the centre circle and text above all arc paths so that
      // after zoom (when the innermost arc animates to y0=0, covering the
      // centre) pointer events still reach the centre circle.
      centreCircle.raise();
      centreG.raise();

      updateBreadcrumbs(root, zoomTo);

      /* ---- Zoom ---- */

      /**
       * Animate the sunburst so `target` fills the full ring.
       * Rescales every node's angular and radial extents relative to
       * the target, storing the result in `d._target`.  A D3 transition
       * then interpolates from `d._current` → `d._target`.
       *
       * @param {d3.HierarchyNode} target - Node to zoom into (or root to zoom out).
       */
      function zoomTo(target) {
        if (!target) target = root;
        currentRoot = target;
        updateBreadcrumbs(target, zoomTo);
        setCentreText(
          target.depth === 0 ? shortDomain(tree.name) : target.data.name,
          target.value + " items"
        );

        // Map the target's angular span to a full circle and its
        // radial origin to zero so the zoomed node fills the ring.
        var t0 = target.x0, t1 = target.x1, ty0 = target.y0;
        var xScale = 2 * Math.PI / (t1 - t0 || 1);
        var yScale = R / (R - ty0 || 1);

        // Pre-compute every node's destination layout coordinates.
        root.each(function (d) {
          d._target = {
            x0: Math.max(0, Math.min(2 * Math.PI, (d.x0 - t0) * xScale)),
            x1: Math.max(0, Math.min(2 * Math.PI, (d.x1 - t0) * xScale)),
            y0: Math.max(0, (d.y0 - ty0) * yScale),
            y1: Math.max(0, (d.y1 - ty0) * yScale),
          };
        });

        var duration = 600;

        paths.transition().duration(duration)
          .attrTween("d", function (d) {
            var ix0 = d3.interpolate(d._current ? d._current.x0 : d.x0, d._target.x0);
            var ix1 = d3.interpolate(d._current ? d._current.x1 : d.x1, d._target.x1);
            var iy0 = d3.interpolate(d._current ? d._current.y0 : d.y0, d._target.y0);
            var iy1 = d3.interpolate(d._current ? d._current.y1 : d.y1, d._target.y1);
            return function (t) {
              d._current = { x0: ix0(t), x1: ix1(t), y0: iy0(t), y1: iy1(t) };
              return arc(d._current);
            };
          })
          .attr("fill-opacity", function (d) {
            return d._target.x1 - d._target.x0 > 0.001 ? 1 : 0;
          });

        labels.transition().duration(duration)
          .attrTween("transform", function (d) {
            var ix0 = d3.interpolate(d._current ? d._current.x0 : d.x0, d._target.x0);
            var ix1 = d3.interpolate(d._current ? d._current.x1 : d.x1, d._target.x1);
            var iy0 = d3.interpolate(d._current ? d._current.y0 : d.y0, d._target.y0);
            var iy1 = d3.interpolate(d._current ? d._current.y1 : d.y1, d._target.y1);
            return function (t) {
              var c = { x0: ix0(t), x1: ix1(t), y0: iy0(t), y1: iy1(t) };
              return labelTransform(c, R);
            };
          })
          .tween("text", function (d) {
            return function () {
              var c = d._current;
              if (!c) return;
              var fits = (c.y1 - c.y0) > 28 && (c.x1 - c.x0) > 0.06;
              if (!fits) { d3.select(this).text(""); return; }
              var name = d.data.name;
              var maxLen = Math.floor((c.y1 - c.y0) / 6);
              d3.select(this).text(
                name.length > maxLen ? name.slice(0, maxLen - 1) + "…" : name
              );
            };
          })
          .attr("fill-opacity", function (d) {
            return d._target.x1 - d._target.x0 > 0.001 ? 1 : 0;
          });

        // Use target._target.y1 (outer edge of the zoomed node's arc) as the
        // centre circle radius — this matches the inner boundary of the first
        // visible ring so the disc fills the hole without any gap.
        centreCircle.attr("r", function () {
          return target.depth === 0 ? (root.y1 || R * 0.18) : target._target ? target._target.y1 || R * 0.18 : R * 0.18;
        });
      }
    }
  };

  // ────────────────────────────────────────────────────────────────
  // 8. Radar / Spider Chart — Quality
  // ────────────────────────────────────────────────────────────────

  /**
   * Multi-axis radar (spider) chart comparing quality metrics across
   * up to ~40 domains (only those with ≥10 pages are eligible).
   * The user picks domains via a checkbox list; each checked domain
   * draws a closed polygon on the same set of radial axes.
   *
   * Quality scores (_q) are normalised to 0–1 from heterogeneous
   * raw values (FK grade, percentages, counts, dates) so they are
   * directly comparable on the same radial scale.
   */
  VIZ.radar = function () {
    fetchJSON(API.domains).then(function (data) {
      hideLoading("loading-radar");

      var eligible = data.filter(function (d) { return d.page_count >= 10; }).slice(0, 40);
      if (!eligible.length) return;

      var maxWords = d3.max(eligible, function (d) { return d.avg_word_count; }) || 1;
      var maxLinks = d3.max(eligible, function (d) { return d.avg_internal_links; }) || 1;

      // Pre-compute a normalised 0–1 quality profile (_q) for each
      // domain.  Each metric uses a different normalisation strategy:
      // FK readability is inverted (lower grade = easier to read),
      // percentages are divided by 100, counts are relative to the
      // dataset maximum, and freshness decays linearly over 730 days.
      eligible.forEach(function (d) {
        d.alt_compliance = d.total_images > 0 ? 100 - d.alt_missing_pct : 100;
        var grade = d.avg_readability || 0;
        var daysSince = d.latest_date
          ? (new Date() - new Date(d.latest_date)) / 86400000
          : 730;
        d._q = {
          fkReadability: grade > 0 ? Math.max(0, Math.min(1, (20 - grade) / 14)) : 0,
          altText:       d.total_images > 0 ? (100 - d.alt_missing_pct) / 100 : 1,
          wordCount:     Math.min(1, d.avg_word_count / maxWords),
          internalLinks: Math.min(1, d.avg_internal_links / maxLinks),
          httpHealth:    Math.max(0, 1 - (d.error_count || 0) / Math.max(d.page_count, 1)),
          freshness:     Math.max(0, Math.min(1, 1 - daysSince / 730)),
          structuredData: Math.min(1, ((d.has_json_ld_pct || 0) + (d.has_microdata_pct || 0)) / 100),
          metadataCompleteness: Math.min(1, (
            (d.top_authors && d.top_authors.length ? 33 : 0) +
            (d.top_publishers && d.top_publishers.length ? 33 : 0) +
            (d.date_count > 0 ? 34 : 0)
          ) / 100),
          extractionCoverage: (d.avg_extraction_coverage || 0) / 100
        };
      });

      var axes = [
        { key: "fkReadability",  label: "FK Reading Ease",   tip: function (d) { return "FK grade " + (d.avg_readability || "\u2013") + " (lower = easier)"; } },
        { key: "altText",        label: "Image Alt Text",    tip: function (d) { return d.total_images > 0 ? d.alt_compliance.toFixed(0) + "% images have alt text" : "No images on site"; } },
        { key: "wordCount",      label: "Content Depth",     tip: function (d) { return fmt(d.avg_word_count) + " avg words/page"; } },
        { key: "internalLinks",  label: "Internal Linking",  tip: function (d) { return d.avg_internal_links.toFixed(1) + " internal links/page"; } },
        { key: "httpHealth",     label: "HTTP Health",       tip: function (d) { return d.error_count + " errors across " + fmt(d.page_count) + " pages"; } },
        { key: "freshness",      label: "Content Freshness", tip: function (d) { return d.latest_date ? "Last updated " + d.latest_date : "No date metadata"; } },
        { key: "structuredData", label: "Structured Data", tip: function (d) { return (d.has_json_ld_pct || 0).toFixed(0) + "% JSON-LD, " + (d.has_microdata_pct || 0).toFixed(0) + "% Microdata"; } },
        { key: "metadataCompleteness", label: "Metadata Depth", tip: function (d) { return (d.top_authors && d.top_authors.length ? "Has authors" : "No authors") + ", " + (d.top_publishers && d.top_publishers.length ? "has publisher" : "no publisher"); } },
        { key: "extractionCoverage", label: "Extraction Coverage", tip: function (d) { return (d.avg_extraction_coverage || 0).toFixed(1) + "% avg fields populated"; } }
      ];

      // Colour by index in the eligible array so each domain keeps
      // its swatch colour stable regardless of checkbox order.
      var stableColour = d3.scaleOrdinal(CSJ_PALETTE);

      // Build the checkbox picker; first three domains pre-selected.
      var pickerEl = document.getElementById("radar-domains");
      pickerEl.innerHTML = "";
      eligible.forEach(function (d, i) {
        var lbl = document.createElement("label");
        lbl.className = "radar-pick";
        var cb = document.createElement("input");
        cb.type = "checkbox";
        cb.value = d.domain;
        if (i < 3) cb.checked = true;
        var sw = document.createElement("span");
        sw.className = "radar-swatch";
        sw.style.background = stableColour(i);
        lbl.appendChild(cb);
        lbl.appendChild(sw);
        lbl.appendChild(document.createTextNode(shortDomain(d.domain) + " (" + d.page_count + ")"));
        pickerEl.appendChild(lbl);
        cb.addEventListener("change", draw);
      });

      /** Redraw the radar SVG from the current checkbox state. */
      function draw() {
        d3.select("#viz-radar").selectAll("svg").remove();
        var selected = [];
        pickerEl.querySelectorAll("input:checked").forEach(function (cb) {
          selected.push(cb.value);
        });
        var chosen = eligible.filter(function (d) { return selected.includes(d.domain); });
        if (!chosen.length) return;

        var sz = vizSize("viz-radar");
        var W = Math.min(sz.w, 720), H = W;
        var R = W / 2 - 90;
        // Equal angular spacing between quality axes.
        var angleSlice = 2 * Math.PI / axes.length;

        var svg = d3.select("#viz-radar").append("svg")
          .attr("width", W).attr("height", H)
          .append("g").attr("transform", "translate(" + W / 2 + "," + H / 2 + ")");

        [0.25, 0.5, 0.75, 1.0].forEach(function (lv) {
          svg.append("circle").attr("r", R * lv)
            .attr("fill", "none")
            .attr("stroke", lv === 1 ? "#b0b8bf" : "#e2e6ea")
            .attr("stroke-dasharray", lv < 1 ? "3,4" : "none");
          svg.append("text")
            .attr("x", 3).attr("y", -R * lv + 4)
            .attr("font-size", 9).attr("fill", "#8c9aa0")
            .text(Math.round(lv * 100) + "%");
        });

        axes.forEach(function (ax, i) {
          var angle = angleSlice * i - Math.PI / 2;
          var cx = Math.cos(angle), cy = Math.sin(angle);
          svg.append("line")
            .attr("x1", 0).attr("y1", 0)
            .attr("x2", R * cx).attr("y2", R * cy)
            .attr("stroke", "#d0d5d8");
          svg.append("text")
            .attr("x", (R + 28) * cx).attr("y", (R + 28) * cy)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "central")
            .attr("font-size", 10).attr("font-weight", 600).attr("fill", "#34495e")
            .text(ax.label);
        });

        chosen.forEach(function (d, ci) {
          var idx = eligible.indexOf(d);
          var c = stableColour(idx);

          var pts = axes.map(function (ax, i) {
            var v = d._q[ax.key] || 0;
            var angle = angleSlice * i - Math.PI / 2;
            return { x: R * v * Math.cos(angle), y: R * v * Math.sin(angle), v: v, ax: ax };
          });

          svg.append("path")
            .datum(pts)
            .attr("d", d3.line()
              .x(function (p) { return p.x; })
              .y(function (p) { return p.y; })
              .curve(d3.curveLinearClosed))
            .attr("fill", c).attr("fill-opacity", 0.1)
            .attr("stroke", c).attr("stroke-width", 2.5).attr("stroke-opacity", 0.85);

          pts.forEach(function (p) {
            svg.append("circle")
              .attr("cx", p.x).attr("cy", p.y).attr("r", 4.5)
              .attr("fill", c).attr("stroke", "#fff").attr("stroke-width", 1.5)
              .style("cursor", "pointer")
              .on("mouseover", function (evt) {
                d3.select(this).attr("r", 7);
                showTip(evt, "<strong>" + shortDomain(d.domain) + "</strong><br>" +
                  p.ax.label + ": " + p.ax.tip(d) +
                  "<br>Score: " + Math.round(p.v * 100) + "%");
              })
              .on("mouseout", function () {
                d3.select(this).attr("r", 4.5);
                hideTip();
              });
          });
        });

        buildLegend("legend-radar", chosen.map(function (d) {
          return { label: shortDomain(d.domain), colour: stableColour(eligible.indexOf(d)) };
        }));
      }

      draw();
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 9. Word Cloud — Content Themes
  // ────────────────────────────────────────────────────────────────

  /**
   * Word cloud rendered via d3-cloud (d3.layout.cloud).  Supports
   * three data modes — tags, schema types, and authors — each
   * fetched from a different API endpoint.  The cloud is fully
   * rebuilt on mode change because the word list changes entirely.
   */
  VIZ.wordcloud = function () {
    /**
     * Clear the container and render a cloud from an array of
     * {tag, count} items.
     *
     * @param {Array.<{tag: string, count: number}>} items
     */
    function renderCloud(items) {
      d3.select("#viz-wordcloud").selectAll("*").remove();
      if (!items.length) {
        d3.select("#viz-wordcloud").append("p").attr("class", "viz-desc")
          .style("text-align", "center").style("padding", "60px 0")
          .text("No data for this mode.");
        return;
      }
      var sz = vizSize("viz-wordcloud");
      var W = sz.w, H = Math.max(400, sz.h);
      var maxCount = items[0].count;
      var sizeScale = d3.scaleSqrt().domain([1, maxCount]).range([12, 64]);

      d3.layout.cloud()
        .size([W, H])
        .words(items.map(function (t) { return { text: t.tag, size: sizeScale(t.count), count: t.count }; }))
        .padding(3)
        .rotate(function () { return (~~(Math.random() * 3) - 1) * 30; })
        .font("Arial")
        .fontSize(function (d) { return d.size; })
        .on("end", function (words) {
          var colour = d3.scaleOrdinal(CSJ_PALETTE);
          d3.select("#viz-wordcloud").append("svg")
            .attr("width", W).attr("height", H)
            .append("g").attr("transform", "translate(" + W / 2 + "," + H / 2 + ")")
            .selectAll("text").data(words).enter().append("text")
            .style("font-size", function (d) { return d.size + "px"; })
            .style("font-family", "Arial")
            .style("font-weight", function (d) { return d.size > 30 ? "700" : "400"; })
            .attr("text-anchor", "middle")
            .attr("fill", function (d, i) { return colour(i); })
            .attr("transform", function (d) { return "translate(" + d.x + "," + d.y + ")rotate(" + d.rotate + ")"; })
            .text(function (d) { return d.text; })
            .on("mouseover", function (evt, d) {
              showTip(evt, "<strong>" + d.text + "</strong><br>Frequency: " + fmt(d.count));
            })
            .on("mouseout", hideTip);
        })
        .start();
    }

    /** Read the mode dropdown and fetch the appropriate endpoint. */
    function loadAndRender() {
      var mode = document.getElementById("wordcloud-mode").value;
      if (mode === "tags") {
        fetchJSON(API.tags).then(function (data) {
          hideLoading("loading-wordcloud");
          renderCloud(data.tags.slice(0, 100));
        });
      } else if (mode === "schema") {
        fetchJSON(API.technology).then(function (data) {
          hideLoading("loading-wordcloud");
          var items = (data.schema_type_frequency || []).map(function (t) {
            return { tag: t.type, count: t.count };
          });
          renderCloud(items.slice(0, 80));
        });
      } else if (mode === "authors") {
        fetchJSON(API.authorship).then(function (data) {
          hideLoading("loading-wordcloud");
          var items = (data.authors || []).map(function (a) {
            return { tag: a.author, count: a.total_pages };
          });
          renderCloud(items.slice(0, 80));
        });
      }
    }

    loadAndRender();
    document.getElementById("wordcloud-mode").addEventListener("change", function () {
      loadAndRender();
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 11. Parallel Coordinates
  // ────────────────────────────────────────────────────────────────

  /**
   * Parallel coordinates plot: one vertical axis per numeric metric,
   * one polyline per domain.  Hovering a line raises it to the top
   * and fades the rest so the user can trace a single domain's
   * profile across all dimensions.
   */
  VIZ.parallel = function () {
    fetchJSON(API.domains).then(function (data) {
      hideLoading("loading-parallel");
      _assignOwnerColours(data);
      var top = data.filter(function (d) { return d.page_count >= 5; }).slice(0, 60);
      if (!top.length) return;

      var dims = [
        { key: "page_count", label: "Pages" },
        { key: "avg_word_count", label: "Avg Words" },
        { key: "avg_readability", label: "Readability" },
        { key: "error_count", label: "Errors" },
        { key: "total_assets", label: "Assets" },
        { key: "avg_internal_links", label: "Int Links" },
        { key: "avg_external_links", label: "Ext Links" },
        { key: "avg_extraction_coverage", label: "Coverage %" },
        { key: "has_json_ld_pct", label: "JSON-LD %" },
        { key: "has_microdata_pct", label: "Microdata %" }
      ];

      var margin = { top: 30, right: 30, bottom: 20, left: 30 };
      var sz = vizSize("viz-parallel");
      var W = sz.w, H = 450;
      var innerW = W - margin.left - margin.right;
      var innerH = H - margin.top - margin.bottom;

      var x = d3.scalePoint().domain(dims.map(function (d) { return d.key; })).range([0, innerW]);
      var y = {};
      dims.forEach(function (dim) {
        var ext = d3.extent(top, function (d) { return d[dim.key] || 0; });
        if (ext[0] === ext[1]) ext[1] = ext[0] + 1;
        y[dim.key] = d3.scaleLinear().domain(ext).range([innerH, 0]);
      });

      var svg = d3.select("#viz-parallel").append("svg").attr("width", W).attr("height", H);
      var g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      /**
       * Generate an SVG path string for a domain's polyline across all axes.
       * @param {Object} d - Domain summary object.
       * @returns {string} SVG path.
       */
      function path(d) {
        return d3.line()(dims.map(function (dim) { return [x(dim.key), y[dim.key](d[dim.key] || 0)]; }));
      }

      g.selectAll(".pc-line").data(top).enter().append("path")
        .attr("class", "pc-line")
        .attr("d", path)
        .attr("fill", "none")
        .attr("stroke", function (d) { return ownerColour(d.ownership); })
        .attr("stroke-opacity", 0.45)
        .attr("stroke-width", 1.5)
        .on("mouseover", function (evt, d) {
          g.selectAll(".pc-line").attr("stroke-opacity", 0.08);
          d3.select(this).raise().attr("stroke-opacity", 1).attr("stroke-width", 3);
          showTip(evt, "<strong>" + shortDomain(d.domain) + "</strong> (" + d.ownership + ")<br>" +
            dims.map(function (dim) { return dim.label + ": " + (d[dim.key] || 0); }).join("<br>"));
        })
        .on("mouseout", function () {
          g.selectAll(".pc-line").attr("stroke-opacity", 0.45).attr("stroke-width", 1.5);
          hideTip();
        });

      dims.forEach(function (dim) {
        var axisG = g.append("g").attr("transform", "translate(" + x(dim.key) + ",0)");
        axisG.call(d3.axisLeft(y[dim.key]).ticks(5).tickFormat(d3.format("~s")));
        axisG.append("text")
          .attr("y", -12).attr("text-anchor", "middle")
          .attr("font-size", 11).attr("font-weight", 600).attr("fill", "#1A1A1A")
          .text(dim.label);
      });

      buildLegend("legend-parallel", Object.keys(OWNER_COLOURS).map(function (k) {
        return { label: k, colour: OWNER_COLOURS[k] };
      }));
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 12. Bubble Chart — Nested Zoomable Circle Pack
  // ────────────────────────────────────────────────────────────────

  /**
   * Zoomable circle-pack where circle area is proportional to page
   * count.  Two grouping modes: "domain" (domains → content kinds)
   * and "content" (content kinds → domains).  Zoom is implemented
   * with d3.interpolateZoom for a smooth geometric transition.
   */
  VIZ.bubble = function () {
    fetchJSON(API.domains).then(function (data) {
      hideLoading("loading-bubble");
      if (!data.length) return;
      _assignOwnerColours(data);

      var CONTENT_COLOURS = {};
      var _contentIdx = 0;
      var CONTENT_PALETTE = [
        "#3A6B7E","#C4841D","#2D6A4F","#A4243B","#7B6D53",
        "#5A5246","#8B5E3C","#6B4226","#4A6741","#9B7042",
        "#B07D3A","#3D5A4C","#7A4F5A","#5C7A6B","#1A1A1A"
      ];
      /** @param {string} kind @returns {string} Lazily-assigned hex colour. */
      function contentColour(kind) {
        if (!CONTENT_COLOURS[kind]) {
          CONTENT_COLOURS[kind] = CONTENT_PALETTE[_contentIdx % CONTENT_PALETTE.length];
          _contentIdx++;
        }
        return CONTENT_COLOURS[kind];
      }

      data.forEach(function (d) {
        Object.keys(d.content_kinds || {}).forEach(function (k) { contentColour(k); });
      });

      /**
       * Build a two-level hierarchy for d3.pack.  "domain" mode:
       * domain → content kinds.  "content" mode: content kind → domains.
       *
       * @param {string} mode - "domain" or "content".
       * @returns {Object} Nested tree suitable for d3.hierarchy().
       */
      function buildHierarchy(mode) {
        if (mode === "content") {
          var byKind = {};
          data.forEach(function (d) {
            var kinds = d.content_kinds || {};
            Object.keys(kinds).forEach(function (k) {
              if (!byKind[k]) byKind[k] = [];
              byKind[k].push({
                name: d.domain,
                value: kinds[k],
                domain: d.domain,
                ownership: d.ownership,
                content_kind: k,
                page_count: d.page_count,
                total_assets: d.total_assets
              });
            });
          });
          return {
            name: "estate",
            children: Object.keys(byKind).sort().map(function (k) {
              return {
                name: k,
                groupType: "content",
                children: byKind[k].sort(function (a, b) { return b.value - a.value; })
              };
            })
          };
        }
        var byDomain = {};
        data.forEach(function (d) {
          var kinds = d.content_kinds || {};
          var children = Object.keys(kinds).map(function (k) {
            return {
              name: k,
              value: kinds[k],
              domain: d.domain,
              ownership: d.ownership,
              content_kind: k,
              page_count: d.page_count,
              total_assets: d.total_assets
            };
          }).sort(function (a, b) { return b.value - a.value; });
          if (!children.length) {
            children = [{
              name: "(unclassified)",
              value: d.page_count,
              domain: d.domain,
              ownership: d.ownership,
              content_kind: "(unclassified)",
              page_count: d.page_count,
              total_assets: d.total_assets
            }];
          }
          byDomain[d.domain] = {
            name: d.domain,
            groupType: "domain",
            ownership: d.ownership,
            children: children
          };
        });
        return {
          name: "estate",
          children: Object.values(byDomain).sort(function (a, b) {
            var va = a.children.reduce(function (s, c) { return s + (c.value || 0); }, 0);
            var vb = b.children.reduce(function (s, c) { return s + (c.value || 0); }, 0);
            return vb - va;
          })
        };
      }

      /**
       * Tear down and redraw the circle pack for the chosen mode.
       * Resets the content colour map so colours are consistent
       * from index 0 after each redraw.
       *
       * @param {string} mode - "domain" or "content".
       */
      function renderBubble(mode) {
        d3.select("#viz-bubble").selectAll("*").remove();
        var bc = document.querySelector(".bubble-breadcrumb");
        if (bc) bc.remove();

        CONTENT_COLOURS = {};
        _contentIdx = 0;
        data.forEach(function (d) {
          Object.keys(d.content_kinds || {}).forEach(function (k) { contentColour(k); });
        });

        var sz = vizSize("viz-bubble");
        var W = sz.w, H = Math.max(550, sz.h);
        var tree = buildHierarchy(mode);

        var root = d3.hierarchy(tree)
          .sum(function (d) { return d.value || 0; })
          .sort(function (a, b) { return b.value - a.value; });

        d3.pack().size([W, H]).padding(function (d) {
          return d.depth === 0 ? 16 : d.depth === 1 ? 6 : 2;
        })(root);

        var focus = root;  // Currently zoomed-into node.
        var view;          // Current [cx, cy, diameter] of the zoom viewport.

        // Declared here so the SVG click handler can reference it before the
        // zoom behaviour is instantiated below.
        var scrollZoom;

        var svg = d3.select("#viz-bubble").append("svg")
          .attr("width", W).attr("height", H)
          .attr("viewBox", [-W / 2, -H / 2, W, H])
          .style("background", "var(--csj-paper)")
          .on("click", function (evt) {
            zoom(evt, root);
            // Also reset any peripheral scroll/pinch zoom applied on top.
            if (scrollZoom) {
              svg.transition().duration(650).call(scrollZoom.transform, d3.zoomIdentity);
            }
          });

        var g = svg.append("g");

        // ── Scroll-wheel / two-finger pinch zoom ──────────────────────────
        // This overlays the hierarchy click-zoom without replacing it.
        // Mouse-button clicks are excluded from the filter so the drill-down
        // behaviour still works; scroll and touch (pinch) are allowed.
        scrollZoom = d3.zoom()
          .scaleExtent([0.25, 10])
          .filter(function (event) {
            return event.type === "wheel" ||
                   event.type === "touchstart" ||
                   event.type === "touchmove" ||
                   event.type === "touchend";
          })
          .on("zoom", function (event) {
            g.attr("transform", event.transform);
            // Toggle a class so CSS can swap the cursor to indicate a
            // scrolled/pinched state is active.
            var isIdentity = (event.transform.k === 1 &&
                              event.transform.x === 0 &&
                              event.transform.y === 0);
            svg.classed("scroll-zoomed", !isIdentity);
          });

        // Apply zoom behaviour; remove the built-in dblclick handler so it
        // does not conflict with the hierarchy navigation on the SVG.
        svg.call(scrollZoom).on("dblclick.zoom", null);

        var circles = g.selectAll("circle")
          .data(root.descendants())
          .enter().append("circle")
          .attr("fill", function (d) {
            if (!d.parent) return "transparent";
            if (d.children) {
              if (d.data.groupType === "domain") return ownerColour(d.data.ownership) + "20";
              if (d.data.groupType === "content") return contentColour(d.data.name) + "20";
              return "#eee";
            }
            if (mode === "domain") return contentColour(d.data.content_kind);
            return ownerColour(d.data.ownership);
          })
          .attr("fill-opacity", function (d) { return d.children ? 1 : 0.82; })
          .attr("stroke", function (d) {
            if (!d.parent) return "none";
            if (d.children) {
              if (d.data.groupType === "domain") return ownerColour(d.data.ownership);
              if (d.data.groupType === "content") return contentColour(d.data.name);
              return "#ccc";
            }
            return "#fff";
          })
          .attr("stroke-width", function (d) { return d.children ? 1.5 : 0.5; })
          .attr("stroke-opacity", function (d) { return d.children ? 0.4 : 1; })
          .attr("cursor", function (d) { return d.children ? "pointer" : "default"; })
          .on("mouseover", function (evt, d) {
            if (!d.parent) return;
            d3.select(this).attr("stroke-width", d.children ? 2.5 : 1.5)
              .attr("stroke-opacity", 1);
            if (d.children) {
              var label = d.data.groupType === "domain" ? shortDomain(d.data.name) : d.data.name;
              var extra = d.data.groupType === "domain" ? d.data.ownership + "<br>" : "";
              showTip(evt, "<strong>" + label + "</strong><br>" +
                extra + "Pages: " + fmt(d.value) +
                "<br>Sub-groups: " + d.children.length +
                "<br><em>Click to zoom in</em>");
            } else {
              showTip(evt, "<strong>" + shortDomain(d.data.domain) + "</strong><br>" +
                d.data.content_kind + "<br>" +
                d.data.ownership + "<br>" +
                "Pages: " + fmt(d.data.value) +
                (d.data.total_assets ? "<br>Assets: " + fmt(d.data.total_assets) : ""));
            }
          })
          .on("mouseout", function (evt, d) {
            d3.select(this)
              .attr("stroke-width", d.children ? 1.5 : 0.5)
              .attr("stroke-opacity", d.children ? 0.4 : 1);
            hideTip();
          })
          .on("click", function (evt, d) {
            if (d.children) {
              evt.stopPropagation();
              zoom(evt, focus === d ? d.parent || root : d);
            }
          });

        var labels = g.selectAll("text.bubble-label")
          .data(root.descendants())
          .enter().append("text")
          .attr("class", "bubble-label")
          .attr("pointer-events", "none")
          .attr("text-anchor", "middle")
          .attr("dy", function (d) { return d.children ? "-0.1em" : ".35em"; })
          .attr("fill", function (d) { return d.children ? "#1A1A1A" : "#fff"; })
          .attr("font-weight", function (d) { return d.children ? 700 : 400; })
          .style("display", function (d) {
            if (!d.parent) return "none";
            return d.parent === root ? "inline" : "none";
          })
          .text(function (d) {
            if (!d.parent) return "";
            if (d.children) {
              var n = d.data.groupType === "domain" ? shortDomain(d.data.name) : d.data.name;
              return n.length > 20 ? n.slice(0, 18) + "\u2026" : n;
            }
            if (mode === "domain") {
              var kind = d.data.content_kind;
              return kind.length > 16 ? kind.slice(0, 14) + "\u2026" : kind;
            }
            return shortDomain(d.data.domain);
          });

        var countLabels = g.selectAll("text.bubble-count")
          .data(root.descendants().filter(function (d) { return d.children && d.parent; }))
          .enter().append("text")
          .attr("class", "bubble-count")
          .attr("pointer-events", "none")
          .attr("text-anchor", "middle")
          .attr("dy", "1.2em")
          .attr("fill", "#5A5246")
          .attr("font-size", 10)
          .style("display", function (d) {
            return d.parent === root ? "inline" : "none";
          })
          .text(function (d) { return fmt(d.value) + " pages"; });

        // Initial layout — centre on root with full extent visible.
        zoomTo([root.x, root.y, root.r * 2]);
        updateBreadcrumbs(root);

        /**
         * Immediately reposition all circles, labels, and count labels
         * to reflect a given viewport [cx, cy, diameter].
         *
         * @param {number[]} v - [centreX, centreY, diameter].
         */
        function zoomTo(v) {
          var k = W / v[2];
          view = v;
          labels.attr("transform", function (d) {
            return "translate(" + ((d.x - v[0]) * k) + "," + ((d.y - v[1]) * k) + ")";
          });
          countLabels.attr("transform", function (d) {
            return "translate(" + ((d.x - v[0]) * k) + "," + ((d.y - v[1]) * k) + ")";
          });
          circles.attr("transform", function (d) {
            return "translate(" + ((d.x - v[0]) * k) + "," + ((d.y - v[1]) * k) + ")";
          });
          circles.attr("r", function (d) { return d.r * k; });
        }

        /**
         * Animate a smooth zoom transition to `target` using
         * d3.interpolateZoom (Bézier-based geometric interpolation).
         *
         * @param {Event|null}       evt    - The triggering DOM event (unused).
         * @param {d3.HierarchyNode} target - Node to zoom into.
         */
        function zoom(evt, target) {
          focus = target;
          hideTip();
          updateBreadcrumbs(target);

          var transition = svg.transition()
            .duration(650)
            .tween("zoom", function () {
              var i = d3.interpolateZoom(view, [target.x, target.y, target.r * 2]);
              return function (t) { zoomTo(i(t)); };
            });

          labels
            .transition(transition)
            .style("display", function (d) {
              if (!d.parent) return "none";
              if (d.parent === target) return "inline";
              if (d === target && d.children) return "inline";
              return "none";
            })
            .attr("font-size", function (d) {
              if (!d.parent) return 0;
              if (d.children) {
                var kk = W / (target.r * 2);
                var rPx = d.r * kk;
                return Math.min(Math.max(rPx / 5, 9), 14);
              }
              var kk2 = W / (target.r * 2);
              var rPx2 = d.r * kk2;
              return Math.min(Math.max(rPx2 / 3.5, 8), 12);
            })
            .style("opacity", function (d) {
              if (!d.parent) return 0;
              if (d.children) return 1;
              var kk = W / (target.r * 2);
              return d.r * kk > 14 ? 1 : 0;
            });

          countLabels
            .transition(transition)
            .style("display", function (d) {
              return d.parent === target ? "inline" : "none";
            })
            .attr("font-size", function (d) {
              var kk = W / (target.r * 2);
              var rPx = d.r * kk;
              return Math.min(Math.max(rPx / 6, 8), 11);
            });
        }

        /**
         * Build a breadcrumb bar above the bubble SVG showing the
         * current zoom path (root → group → …).
         * @param {d3.HierarchyNode} node - Current zoom target.
         */
        function updateBreadcrumbs(node) {
          var container = document.getElementById("viz-bubble").parentNode;
          var existing = container.querySelector(".bubble-breadcrumb");
          if (existing) existing.remove();

          if (node === root) return;

          var chain = [];
          var cur = node;
          while (cur) { chain.unshift(cur); cur = cur.parent; }

          var bar = document.createElement("div");
          bar.className = "bubble-breadcrumb";

          chain.forEach(function (n, i) {
            if (i > 0) {
              var sep = document.createElement("span");
              sep.className = "bubble-crumb-sep";
              sep.textContent = "\u203A";
              bar.appendChild(sep);
            }
            var btn = document.createElement("span");
            var isLast = i === chain.length - 1;
            btn.className = "bubble-crumb" + (isLast ? " current" : "");
            if (n === root) {
              btn.textContent = "All";
            } else if (n.data.groupType === "domain") {
              btn.textContent = shortDomain(n.data.name);
            } else if (n.data.groupType === "content") {
              btn.textContent = n.data.name;
            } else {
              btn.textContent = n.data.name;
            }
            if (!isLast) {
              (function (target) {
                btn.addEventListener("click", function () { zoom(null, target); });
              })(n);
            }
            bar.appendChild(btn);
          });

          var vizCanvas = document.getElementById("viz-bubble");
          vizCanvas.parentNode.insertBefore(bar, vizCanvas);
        }

        if (mode === "domain") {
          buildLegend("legend-bubble",
            Object.keys(CONTENT_COLOURS).sort().map(function (k) {
              return { label: k, colour: CONTENT_COLOURS[k] };
            })
          );
        } else {
          buildLegend("legend-bubble",
            Object.keys(OWNER_COLOURS).map(function (k) {
              return { label: k, colour: OWNER_COLOURS[k] };
            })
          );
        }
      }

      var currentMode = document.getElementById("bubble-grouping").value || "domain";
      renderBubble(currentMode);

      document.getElementById("bubble-grouping").addEventListener("change", function () {
        currentMode = this.value;
        renderBubble(currentMode);
      });
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 13. Content Types by Domain
  // ────────────────────────────────────────────────────────────────

  /**
   * Horizontal stacked bar chart where each segment is a content
   * kind (e.g. "article", "service", "form").  Sortable by page
   * count, content-kind diversity, or alphabetically.
   */
  VIZ.contenttypes = function () {
    fetchJSON(API.domains).then(function (data) {
      hideLoading("loading-contenttypes");

      var kindSet = new Set();
      data.forEach(function (d) {
        Object.keys(d.content_kinds || {}).forEach(function (k) { kindSet.add(k); });
      });
      var allKinds = Array.from(kindSet).sort();
      if (!allKinds.length) return;

      var top = data.filter(function (d) { return d.page_count >= 2; }).slice(0, 50);
      if (!top.length) return;

      var kindColour = d3.scaleOrdinal()
        .domain(allKinds)
        .range(CSJ_PALETTE.concat(d3.schemeTableau10));

      /**
       * Sort the top-50 slice in place.
       * @param {string} mode - "diversity" | "alpha" | "pages".
       */
      function sortData(mode) {
        if (mode === "diversity") {
          top.sort(function (a, b) {
            return Object.keys(b.content_kinds || {}).length - Object.keys(a.content_kinds || {}).length;
          });
        } else if (mode === "alpha") {
          top.sort(function (a, b) { return a.domain.localeCompare(b.domain); });
        } else {
          top.sort(function (a, b) { return b.page_count - a.page_count; });
        }
      }

      /** Clear and redraw bars with the currently selected sort order. */
      function draw() {
        d3.select("#viz-contenttypes").selectAll("*").remove();
        var sortMode = document.getElementById("contenttypes-sort").value || "pages";
        sortData(sortMode);

        var barH = 22, gap = 3;
        var margin = { top: 10, right: 30, bottom: 10, left: 200 };
        var sz = vizSize("viz-contenttypes");
        var W = sz.w;
        var innerW = W - margin.left - margin.right;
        var H = margin.top + margin.bottom + top.length * (barH + gap);

        var maxPages = d3.max(top, function (d) { return d.page_count; }) || 1;
        var x = d3.scaleLinear().domain([0, maxPages]).range([0, innerW]);

        var svg = d3.select("#viz-contenttypes").append("svg").attr("width", W).attr("height", H);
        var g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        top.forEach(function (d, i) {
          var y = i * (barH + gap);

          g.append("text")
            .attr("x", -4).attr("y", y + barH / 2 + 4)
            .attr("text-anchor", "end").attr("font-size", 11).attr("fill", "#1A1A1A")
            .text(shortDomain(d.domain));

          var cumX = 0;
          var kinds = d.content_kinds || {};
          var sortedKinds = Object.keys(kinds).sort(function (a, b) { return kinds[b] - kinds[a]; });

          sortedKinds.forEach(function (kind) {
            var count = kinds[kind];
            var w = x(count);
            if (w < 0.5) return;

            g.append("rect")
              .attr("x", cumX).attr("y", y)
              .attr("width", w).attr("height", barH)
              .attr("fill", kindColour(kind))
              .attr("rx", 1)
              .on("mouseover", function (evt) {
                var pct = (count / d.page_count * 100).toFixed(1);
                showTip(evt,
                  "<strong>" + shortDomain(d.domain) + "</strong><br>" +
                  kind + ": " + fmt(count) + " pages (" + pct + "%)" +
                  "<br>Total pages: " + fmt(d.page_count) +
                  "<br>Content types: " + Object.keys(kinds).length);
              })
              .on("mouseout", hideTip);
            cumX += w;
          });

          g.append("text")
            .attr("x", cumX + 4).attr("y", y + barH / 2 + 4)
            .attr("font-size", 10).attr("fill", "#5A5246")
            .text(fmt(d.page_count));
        });

        var usedKinds = new Set();
        top.forEach(function (d) {
          Object.keys(d.content_kinds || {}).forEach(function (k) { usedKinds.add(k); });
        });
        buildLegend("legend-contenttypes",
          Array.from(usedKinds).sort().map(function (k) {
            return { label: k, colour: kindColour(k) };
          })
        );
      }

      draw();

      document.getElementById("contenttypes-sort").addEventListener("change", function () {
        draw();
      });
    });
  };

  // ────────────────────────────────────────────────────────────────
  // GLOBAL FILTER SYSTEM
  // ────────────────────────────────────────────────────────────────
  //
  // Four custom multi-select widgets replace the old single-select
  // dropdowns.  Each widget manages its own Set of selected values
  // and calls onFilterChange() when the selection changes.
  //
  // The filter bar also has a plain number input for min_coverage.
  //
  // When any filter changes:
  //  1. `activeFilters` is rebuilt from the widget selections.
  //  2. `cache` and `rendered` are cleared so every panel
  //     re-fetches with the updated query-string on next activation.
  //  3. The currently visible panel is re-rendered immediately.
  //
  // `fetchJSON` is monkey-patched to transparently append filter
  // params to every API call — individual VIZ functions are unaware.

  /** Current filter state; values are comma-separated strings. */
  var activeFilters = {};
  var filterOptionsData = null;

  /**
   * Build a custom multi-select widget backed by a Set.
   *
   * @param {string} containerId  — id of the `.multi-select` wrapper div.
   * @param {string} placeholder  — button label when nothing is selected.
   * @returns {{ getValues, setValues, clear, populate }}
   */
  function makeMultiSelect(containerId, placeholder) {
    var container = document.getElementById(containerId);
    var btn       = container.querySelector(".ms-btn");
    var label     = container.querySelector(".ms-label");
    var panel     = container.querySelector(".ms-panel");
    var selected  = new Set();

    function _updateBtn() {
      var existing = btn.querySelector(".ms-count");
      if (selected.size === 0) {
        label.textContent = placeholder;
        if (existing) existing.remove();
      } else {
        if (selected.size === 1) {
          var val = Array.from(selected)[0];
          var found = null;
          panel.querySelectorAll(".ms-item").forEach(function (el) {
            if (el.dataset.value === val) found = el;
          });
          label.textContent = found ? found.querySelector(".ms-item-text").textContent : val;
        } else {
          label.textContent = selected.size + " selected";
        }
        var cnt = existing;
        if (!cnt) {
          cnt = document.createElement("span");
          cnt.className = "ms-count";
          btn.insertBefore(cnt, btn.querySelector(".ms-arrow"));
        }
        cnt.textContent = selected.size;
      }
    }

    function getValues() { return Array.from(selected); }

    function setValues(vals) {
      selected = new Set(vals);
      panel.querySelectorAll("input[type='checkbox']").forEach(function (cb) {
        cb.checked = selected.has(cb.value);
      });
      _updateBtn();
    }

    function clear() { setValues([]); }

    function populate(options) {
      var prev = Array.from(selected);
      panel.innerHTML = "";
      options.forEach(function (opt) {
        var lbl = document.createElement("label");
        lbl.className = "ms-item";
        lbl.dataset.value = opt.value;
        var cb = document.createElement("input");
        cb.type = "checkbox";
        cb.value = opt.value;
        cb.checked = selected.has(opt.value);
        cb.addEventListener("change", function () {
          if (this.checked) selected.add(this.value);
          else selected.delete(this.value);
          _updateBtn();
          onFilterChange();
        });
        var span = document.createElement("span");
        span.className = "ms-item-text";
        span.textContent = opt.label;
        lbl.appendChild(cb);
        lbl.appendChild(span);
        panel.appendChild(lbl);
      });
      // Restore only values that still exist in the new options.
      var validVals = new Set(options.map(function (o) { return o.value; }));
      selected = new Set(prev.filter(function (v) { return validVals.has(v); }));
      panel.querySelectorAll("input").forEach(function (cb) {
        cb.checked = selected.has(cb.value);
      });
      _updateBtn();
    }

    // Prevent clicks inside the panel from reaching the document close handler.
    panel.addEventListener("click", function (e) {
      e.stopPropagation();
    });

    // Toggle panel open/close on button click.
    btn.addEventListener("click", function (e) {
      e.stopPropagation();
      var isOpen = panel.style.display !== "none";
      document.querySelectorAll(".ms-panel").forEach(function (p) {
        p.style.display = "none";
        p.closest(".multi-select").querySelector(".ms-btn").classList.remove("ms-open");
      });
      if (!isOpen) {
        panel.style.display = "";
        btn.classList.add("ms-open");
      }
    });

    _updateBtn();
    return { getValues: getValues, setValues: setValues, clear: clear, populate: populate };
  }

  // Close all panels when clicking outside.
  document.addEventListener("click", function () {
    document.querySelectorAll(".ms-panel").forEach(function (p) {
      p.style.display = "none";
      p.closest(".multi-select").querySelector(".ms-btn").classList.remove("ms-open");
    });
  });

  // Instantiate the four multi-select widgets.
  var msRuns   = makeMultiSelect("ms-runs",   "All runs");
  var msCms    = makeMultiSelect("ms-cms",    "All platforms");
  var msKind   = makeMultiSelect("ms-kind",   "All types");
  var msSchema = makeMultiSelect("ms-schema", "All formats");

  // Populate runs from the server-rendered list embedded in the page.
  msRuns.populate((window.ECO_RUNS_LIST || []).map(function (r) {
    return { value: r.name, label: r.label + " (" + r.page_count + " pages)" };
  }));

  // Schema options are static.
  msSchema.populate([
    { value: "json_ld",    label: "JSON-LD" },
    { value: "microdata",  label: "Microdata" },
    { value: "rdfa",       label: "RDFa" }
  ]);

  // Apply pre-selected run(s) from the URL query string.
  if (window.ECO_PRESELECTED_RUNS) {
    activeFilters.runs = window.ECO_PRESELECTED_RUNS;
    msRuns.setValues(activeFilters.runs.split(",").map(function (s) { return s.trim(); }));
  }

  /**
   * Serialise `activeFilters` into a URL query string.
   * @returns {string}
   */
  function buildFilterQuery() {
    var parts = [];
    if (activeFilters.runs)           parts.push("runs="           + encodeURIComponent(activeFilters.runs));
    if (activeFilters.cms)            parts.push("cms="            + encodeURIComponent(activeFilters.cms));
    if (activeFilters.content_kinds)  parts.push("content_kinds="  + encodeURIComponent(activeFilters.content_kinds));
    if (activeFilters.schema_formats) parts.push("schema_formats=" + encodeURIComponent(activeFilters.schema_formats));
    if (activeFilters.min_coverage)   parts.push("min_coverage="   + activeFilters.min_coverage);
    return parts.length ? "?" + parts.join("&") : "";
  }

  /**
   * Append filter params to a URL, respecting existing query strings.
   * @param {string} url
   * @returns {string}
   */
  function addFilterParam(url) {
    var q = buildFilterQuery();
    if (!q) return url;
    return url + (url.indexOf("?") >= 0 ? "&" + q.substring(1) : q);
  }

  // Monkey-patch fetchJSON to inject filter params transparently.
  var _origFetchJSON = fetchJSON;
  fetchJSON = function (url) {
    var filtered = addFilterParam(url);
    if (cache[filtered]) return Promise.resolve(cache[filtered]);
    return d3.json(filtered).then(function (d) { cache[filtered] = d; return d; });
  };

  /**
   * Read all widget selections, rebuild `activeFilters`, flush
   * the cache, and re-render the active panel.
   */
  function onFilterChange() {
    var runs   = msRuns.getValues().join(",");
    var cms    = msCms.getValues().join(",");
    var kind   = msKind.getValues().join(",");
    var schema = msSchema.getValues().join(",");
    var cov    = document.getElementById("filter-coverage").value;

    activeFilters = {};
    if (runs)                          activeFilters.runs           = runs;
    if (cms)                           activeFilters.cms            = cms;
    if (kind)                          activeFilters.content_kinds  = kind;
    if (schema)                        activeFilters.schema_formats = schema;
    if (cov && parseFloat(cov) > 0)    activeFilters.min_coverage   = cov;

    var count = Object.keys(activeFilters).length;
    var badge    = document.getElementById("filterCount");
    var clearBtn = document.getElementById("filterClearAll");
    if (count > 0) {
      badge.textContent = count;
      badge.style.display = "";
      clearBtn.style.display = "";
    } else {
      badge.style.display = "none";
      clearBtn.style.display = "none";
    }

    updateFilterChips();
    cache = {};
    rendered = {};
    reloadFilterOptions();
    var activePanel = document.querySelector(".viz-tab.active");
    if (activePanel) renderPanel(activePanel.dataset.panel);
  }

  /** Render one removable chip per individual active filter value. */
  function updateFilterChips() {
    var el      = document.getElementById("filterChips");
    var section = document.getElementById("filterAppliedSection");
    var chips = [];

    if (activeFilters.runs) {
      activeFilters.runs.split(",").forEach(function (n) {
        var r = (window.ECO_RUNS_LIST || []).find(function (x) { return x.name === n; });
        chips.push({ key: "runs", value: n, label: "Run: " + (r ? r.label : n) });
      });
    }
    if (activeFilters.cms) {
      activeFilters.cms.split(",").forEach(function (v) {
        chips.push({ key: "cms", value: v, label: "CMS: " + v });
      });
    }
    if (activeFilters.content_kinds) {
      activeFilters.content_kinds.split(",").forEach(function (v) {
        chips.push({ key: "content_kinds", value: v, label: "Kind: " + v });
      });
    }
    if (activeFilters.schema_formats) {
      activeFilters.schema_formats.split(",").forEach(function (v) {
        chips.push({ key: "schema_formats", value: v, label: "Schema: " + v });
      });
    }
    if (activeFilters.min_coverage) {
      chips.push({ key: "min_coverage", value: "", label: "Coverage \u2265 " + activeFilters.min_coverage + "%" });
    }

    section.style.display = chips.length > 0 ? "" : "none";
    el.innerHTML = chips.map(function (c) {
      return '<span class="filter-chip">' + c.label +
        ' <span class="filter-chip-x" data-key="' + c.key + '" data-value="' + c.value.replace(/"/g, "&quot;") + '">&times;</span></span>';
    }).join("");
    el.querySelectorAll(".filter-chip-x").forEach(function (x) {
      x.addEventListener("click", function () {
        var key = this.dataset.key;
        var val = this.dataset.value;
        if (key === "runs") {
          msRuns.setValues(msRuns.getValues().filter(function (v) { return v !== val; }));
        } else if (key === "cms") {
          msCms.setValues(msCms.getValues().filter(function (v) { return v !== val; }));
        } else if (key === "content_kinds") {
          msKind.setValues(msKind.getValues().filter(function (v) { return v !== val; }));
        } else if (key === "schema_formats") {
          msSchema.setValues(msSchema.getValues().filter(function (v) { return v !== val; }));
        } else if (key === "min_coverage") {
          document.getElementById("filter-coverage").value = "";
        }
        onFilterChange();
      });
    });
  }

  // ── Filter event wiring ───────────────────────────────────────
  document.getElementById("filter-coverage").addEventListener("change", onFilterChange);
  document.getElementById("filterClearAll").addEventListener("click", function () {
    msRuns.clear();
    msCms.clear();
    msKind.clear();
    msSchema.clear();
    document.getElementById("filter-coverage").value = "";
    onFilterChange();
  });

  /**
   * Reload CMS and content-kind options from the filter_options
   * endpoint, preserving current selections where still valid.
   */
  function reloadFilterOptions() {
    var url = API.filter_options;
    if (activeFilters.runs) {
      url += (url.indexOf("?") >= 0 ? "&" : "?") + "runs=" + encodeURIComponent(activeFilters.runs);
    }
    d3.json(url).then(function (opts) {
      filterOptionsData = opts;
      msCms.populate((opts.cms_values || []).map(function (v) { return { value: v, label: v }; }));
      msKind.populate((opts.content_kinds || []).map(function (v) { return { value: v, label: v }; }));
    });
  }

  // Initial population of CMS and kind dropdowns.
  reloadFilterOptions();

  // Trigger initial filter state if a run was pre-selected.
  if (activeFilters.runs) {
    onFilterChange();
  }


  // ────────────────────────────────────────────────────────────────
  // 14. CMS Landscape (Treemap)
  // ────────────────────────────────────────────────────────────────

  /**
   * Treemap showing CMS/platform distribution across domains.  Uses
   * the `technology` endpoint's `cms_distribution` array.  Structure:
   * root → CMS name → individual domains (leaf, area = page count).
   */
  VIZ.cmslandscape = function () {
    fetchJSON(API.technology).then(function (data) {
      hideLoading("loading-cmslandscape");
      var cms = data.cms_distribution || [];
      if (!cms.length) {
        d3.select("#viz-cmslandscape").append("p").attr("class", "viz-desc")
          .style("text-align", "center").style("padding", "60px 0")
          .text("No CMS/platform data detected. The cms_generator field was empty for all pages.");
        return;
      }

      var sz = vizSize("viz-cmslandscape");
      var W = sz.w, H = sz.h;
      var cmsColour = d3.scaleOrdinal(CSJ_PALETTE);

      var root = d3.hierarchy({
        name: "estate",
        children: cms.map(function (c) {
          return {
            name: c.cms,
            children: c.domains.map(function (d) {
              return { name: d.domain, value: d.pages, cms: c.cms };
            })
          };
        })
      }).sum(function (d) { return d.value || 0; })
        .sort(function (a, b) { return b.value - a.value; });

      d3.treemap().size([W, H]).padding(2).paddingTop(18).round(true)(root);
      var svg = d3.select("#viz-cmslandscape").append("svg").attr("width", W).attr("height", H);

      var groups = svg.selectAll("g").data(root.children).enter().append("g");
      groups.append("rect")
        .attr("x", function (d) { return d.x0; }).attr("y", function (d) { return d.y0; })
        .attr("width", function (d) { return d.x1 - d.x0; })
        .attr("height", function (d) { return d.y1 - d.y0; })
        .attr("fill", "none").attr("stroke", function (d) { return cmsColour(d.data.name); })
        .attr("stroke-width", 2);
      groups.append("text")
        .attr("x", function (d) { return d.x0 + 4; }).attr("y", function (d) { return d.y0 + 13; })
        .text(function (d) { return d.data.name + " (" + fmt(d.value) + " pages)"; })
        .attr("font-size", 11).attr("font-weight", 700)
        .attr("fill", function (d) { return cmsColour(d.data.name); });

      var leaves = svg.selectAll(".leaf").data(root.leaves()).enter().append("g").attr("class", "leaf");
      leaves.append("rect")
        .attr("x", function (d) { return d.x0; }).attr("y", function (d) { return d.y0; })
        .attr("width", function (d) { return d.x1 - d.x0; })
        .attr("height", function (d) { return d.y1 - d.y0; })
        .attr("fill", function (d) { return cmsColour(d.parent.data.name); })
        .attr("fill-opacity", 0.7).attr("stroke", "#fff").attr("stroke-width", 0.5)
        .on("mouseover", function (evt, d) {
          d3.select(this).attr("fill-opacity", 1);
          showTip(evt, "<strong>" + shortDomain(d.data.name) + "</strong><br>" +
            "Platform: " + d.data.cms + "<br>Pages: " + fmt(d.data.value));
        })
        .on("mouseout", function () { d3.select(this).attr("fill-opacity", 0.7); hideTip(); });

      leaves.append("text")
        .attr("x", function (d) { return d.x0 + 3; }).attr("y", function (d) { return d.y0 + 13; })
        .text(function (d) { return (d.x1 - d.x0 > 50 && d.y1 - d.y0 > 16) ? shortDomain(d.data.name) : ""; })
        .attr("font-size", 10).attr("fill", "#fff").attr("pointer-events", "none");

      buildLegend("legend-cmslandscape", cms.map(function (c, i) {
        return { label: c.cms + " (" + c.domain_count + " domains)", colour: cmsColour(c.cms) };
      }));
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 15. Structured Data Adoption Matrix
  // ────────────────────────────────────────────────────────────────

  /**
   * Dot strip chart of structured-data signal adoption.
   * Each row = one signal; each dot = one domain placed at its adoption %.
   * Noindex dots use red to flag it as a warning rather than a positive signal.
   */
  VIZ.structureddata = function () {
    fetchJSON(API.domains).then(function (data) {
      hideLoading("loading-structureddata");
      var signals = ["has_json_ld_pct", "has_microdata_pct", "has_rdfa_pct",
        "has_hreflang_pct", "has_feed_pct", "has_pagination_pct",
        "has_breadcrumb_schema_pct", "robots_noindex_pct"];
      var signalLabels = {
        "has_json_ld_pct": "JSON-LD", "has_microdata_pct": "Microdata",
        "has_rdfa_pct": "RDFa", "has_hreflang_pct": "Hreflang",
        "has_feed_pct": "RSS / Atom Feed", "has_pagination_pct": "Pagination",
        "has_breadcrumb_schema_pct": "Breadcrumb Schema", "robots_noindex_pct": "Noindex"
      };

      var top = data.filter(function (d) { return d.page_count >= 2; }).slice(0, 40);
      if (!top.length) return;

      var sz = vizSize("viz-structureddata");
      var labelW = 180, countW = 90, rowH = 52;
      var margin = { top: 16, right: countW + 16, bottom: 44, left: labelW };
      var W = sz.w, iW = W - margin.left - margin.right;
      var H = margin.top + signals.length * rowH + margin.bottom;

      var svg = d3.select("#viz-structureddata").append("svg").attr("width", W).attr("height", H);
      var g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      var x = d3.scaleLinear().domain([0, 100]).range([0, iW]);

      [0, 25, 50, 75, 100].forEach(function (pct) {
        g.append("line")
          .attr("x1", x(pct)).attr("x2", x(pct))
          .attr("y1", 0).attr("y2", signals.length * rowH)
          .attr("stroke", "#E0D8CC").attr("stroke-width", 1)
          .attr("stroke-dasharray", pct === 0 || pct === 100 ? "none" : "3,3");
      });

      g.append("g").attr("transform", "translate(0," + signals.length * rowH + ")")
        .call(d3.axisBottom(x).tickValues([0, 25, 50, 75, 100]).tickFormat(function (v) { return v + "%"; }))
        .call(function (a) { a.select(".domain").attr("stroke", "#C8C0B4"); })
        .call(function (a) { a.selectAll(".tick line").remove(); })
        .call(function (a) { a.selectAll("text").attr("font-size", 11).attr("fill", "#5A5246"); });

      var jitter = [-16, -8, 0, 8, 16];

      signals.forEach(function (sig, i) {
        var y0 = i * rowH, yMid = y0 + rowH / 2;
        var isWarn = sig === "robots_noindex_pct";

        g.append("rect")
          .attr("x", 0).attr("y", y0).attr("width", iW).attr("height", rowH)
          .attr("fill", i % 2 === 0 ? "rgba(248,244,239,0.6)" : "rgba(255,255,255,0.4)");

        g.append("line")
          .attr("x1", 0).attr("x2", iW).attr("y1", y0).attr("y2", y0)
          .attr("stroke", "#E0D8CC").attr("stroke-width", 1);

        svg.append("text")
          .attr("x", margin.left - 10).attr("y", margin.top + yMid)
          .attr("text-anchor", "end").attr("dominant-baseline", "central")
          .attr("font-size", 12).attr("font-weight", 500)
          .attr("fill", isWarn ? "#8B1A2D" : "#1A1A1A")
          .text(signalLabels[sig] || sig);

        var dotsData = top.filter(function (d) { return (d[sig] || 0) > 0; });
        dotsData.sort(function (a, b) { return (a[sig] || 0) - (b[sig] || 0); });

        dotsData.forEach(function (d, di) {
          var val = d[sig] || 0;
          var yOff = jitter[di % jitter.length];
          g.append("circle")
            .attr("cx", x(val)).attr("cy", yMid + yOff).attr("r", 5)
            .attr("fill", isWarn ? "#C0392B" : "#3B7DB5")
            .attr("fill-opacity", 0.72)
            .attr("stroke", isWarn ? "#8B1A2D" : "#2B5D90").attr("stroke-width", 0.8)
            .on("mouseover", function (evt) {
              showTip(evt, "<strong>" + shortDomain(d.domain) + "</strong><br>" +
                (signalLabels[sig] || sig) + ": " + val.toFixed(1) + "% of " + fmt(d.page_count) + " pages");
            })
            .on("mouseout", hideTip);
        });

        var adoptedCount = dotsData.length;
        svg.append("text")
          .attr("x", margin.left + iW + 10).attr("y", margin.top + yMid)
          .attr("dominant-baseline", "central").attr("font-size", 11).attr("fill", "#5A5246")
          .text(adoptedCount + "\u202f/\u202f" + top.length + " domains");
      });

      buildLegend("legend-structureddata", [
        { colour: "#3B7DB5", label: "Signal detected (% of pages)" },
        { colour: "#C0392B", label: "Noindex rate (warning)" },
        { colour: "#E0D8CC", label: "Not detected (absent from chart)" }
      ]);
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 16. SEO Readiness Scorecard
  // ────────────────────────────────────────────────────────────────

  /**
   * Dot strip chart scoring each domain against SEO best-practice checks.
   * Each row = one check; each dot = one domain placed at its pass-rate %.
   * Green ≥80 %, amber ≥30 %, red <30 %. Not-detected domains are omitted.
   */
  VIZ.seoreadiness = function () {
    fetchJSON(API.technology).then(function (data) {
      hideLoading("loading-seoreadiness");
      var seo = (data.seo_readiness || []).slice(0, 40);
      if (!seo.length) return;

      var checks = ["has_canonical", "has_structured_data", "has_breadcrumb_schema",
        "has_hreflang", "has_feed", "has_pagination", "has_robots"];
      var checkLabels = {
        "has_canonical": "Canonical", "has_structured_data": "Structured Data",
        "has_breadcrumb_schema": "Breadcrumb Schema", "has_hreflang": "Hreflang",
        "has_feed": "RSS / Atom Feed", "has_pagination": "Pagination", "has_robots": "Robots"
      };

      var sz = vizSize("viz-seoreadiness");
      var labelW = 180, countW = 90, rowH = 52;
      var margin = { top: 16, right: countW + 16, bottom: 44, left: labelW };
      var W = sz.w, iW = W - margin.left - margin.right;
      var H = margin.top + checks.length * rowH + margin.bottom;

      var svg = d3.select("#viz-seoreadiness").append("svg").attr("width", W).attr("height", H);
      var g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      var x = d3.scaleLinear().domain([0, 100]).range([0, iW]);

      [0, 25, 50, 75, 100].forEach(function (pct) {
        g.append("line")
          .attr("x1", x(pct)).attr("x2", x(pct))
          .attr("y1", 0).attr("y2", checks.length * rowH)
          .attr("stroke", "#E0D8CC").attr("stroke-width", 1)
          .attr("stroke-dasharray", pct === 0 || pct === 100 ? "none" : "3,3");
      });

      g.append("g").attr("transform", "translate(0," + checks.length * rowH + ")")
        .call(d3.axisBottom(x).tickValues([0, 25, 50, 75, 100]).tickFormat(function (v) { return v + "%"; }))
        .call(function (a) { a.select(".domain").attr("stroke", "#C8C0B4"); })
        .call(function (a) { a.selectAll(".tick line").remove(); })
        .call(function (a) { a.selectAll("text").attr("font-size", 11).attr("fill", "#5A5246"); });

      var jitter = [-16, -8, 0, 8, 16];

      checks.forEach(function (chk, i) {
        var y0 = i * rowH, yMid = y0 + rowH / 2;

        g.append("rect")
          .attr("x", 0).attr("y", y0).attr("width", iW).attr("height", rowH)
          .attr("fill", i % 2 === 0 ? "rgba(248,244,239,0.6)" : "rgba(255,255,255,0.4)");

        g.append("line")
          .attr("x1", 0).attr("x2", iW).attr("y1", y0).attr("y2", y0)
          .attr("stroke", "#E0D8CC").attr("stroke-width", 1);

        svg.append("text")
          .attr("x", margin.left - 10).attr("y", margin.top + yMid)
          .attr("text-anchor", "end").attr("dominant-baseline", "central")
          .attr("font-size", 12).attr("font-weight", 500).attr("fill", "#1A1A1A")
          .text(checkLabels[chk] || chk);

        var dotsData = seo.filter(function (d) { return (d[chk] || 0) > 0; });
        dotsData.sort(function (a, b) {
          var pa = (a[chk] || 0) / (a.pages || 1) * 100;
          var pb = (b[chk] || 0) / (b.pages || 1) * 100;
          return pa - pb;
        });

        dotsData.forEach(function (d, di) {
          var pc = d.pages || 1;
          var pct = (d[chk] || 0) / pc * 100;
          var colour = pct >= 80 ? "#2D6A4F" : pct >= 30 ? "#C4841D" : "#A4243B";
          var strokeC = pct >= 80 ? "#1A4030" : pct >= 30 ? "#8B5A10" : "#6B1525";
          var yOff = jitter[di % jitter.length];
          g.append("circle")
            .attr("cx", x(pct)).attr("cy", yMid + yOff).attr("r", 5)
            .attr("fill", colour).attr("fill-opacity", 0.8)
            .attr("stroke", strokeC).attr("stroke-width", 0.8)
            .on("mouseover", function (evt) {
              showTip(evt, "<strong>" + shortDomain(d.domain) + "</strong><br>" +
                (checkLabels[chk] || chk) + ": " + (d[chk] || 0) + "\u202f/\u202f" + pc + " pages (" + pct.toFixed(1) + "%)");
            })
            .on("mouseout", hideTip);
        });

        var adoptedCount = dotsData.length;
        svg.append("text")
          .attr("x", margin.left + iW + 10).attr("y", margin.top + yMid)
          .attr("dominant-baseline", "central").attr("font-size", 11).attr("fill", "#5A5246")
          .text(adoptedCount + "\u202f/\u202f" + seo.length + " domains");
      });

      buildLegend("legend-seoreadiness", [
        { colour: "#2D6A4F", label: "\u226580% of pages pass" },
        { colour: "#C4841D", label: "30\u201379% of pages pass" },
        { colour: "#A4243B", label: "<30% of pages pass" },
        { colour: "#E0D8CC", label: "Not detected (absent from chart)" }
      ]);
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 17. Extraction Coverage Histogram
  // ────────────────────────────────────────────────────────────────

  /**
   * Vertical bar histogram of extraction-coverage buckets (% of
   * metadata fields populated per page).  Data comes from the
   * `technology` endpoint's `coverage_histogram` array.
   */
  VIZ.coverage = function () {
    fetchJSON(API.technology).then(function (data) {
      hideLoading("loading-coverage");
      var hist = data.coverage_histogram || [];
      if (!hist.length) return;

      var sz = vizSize("viz-coverage");
      var margin = { top: 20, right: 20, bottom: 40, left: 50 };
      var W = sz.w, H = 350;
      var iW = W - margin.left - margin.right;
      var iH = H - margin.top - margin.bottom;

      var svg = d3.select("#viz-coverage").append("svg").attr("width", W).attr("height", H);
      var g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      var x = d3.scaleBand().domain(hist.map(function (h) { return h.bucket; })).range([0, iW]).padding(0.15);
      var maxCount = d3.max(hist, function (h) { return h.count; }) || 1;
      var y = d3.scaleLinear().domain([0, maxCount]).range([iH, 0]);
      var colourScale = d3.scaleSequential(d3.interpolateBlues).domain([0, 90]);

      g.selectAll("rect").data(hist).enter().append("rect")
        .attr("x", function (d) { return x(d.bucket); })
        .attr("y", function (d) { return y(d.count); })
        .attr("width", x.bandwidth())
        .attr("height", function (d) { return iH - y(d.count); })
        .attr("fill", function (d) {
          var mid = parseInt(d.bucket);
          return colourScale(mid);
        })
        .attr("rx", 2)
        .on("mouseover", function (evt, d) {
          showTip(evt, "<strong>" + d.bucket + "</strong><br>" + fmt(d.count) + " pages");
        })
        .on("mouseout", hideTip);

      g.selectAll("text.bar-label").data(hist).enter().append("text")
        .attr("x", function (d) { return x(d.bucket) + x.bandwidth() / 2; })
        .attr("y", function (d) { return y(d.count) - 4; })
        .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", "#5A5246")
        .text(function (d) { return d.count > 0 ? fmt(d.count) : ""; });

      g.append("g").attr("transform", "translate(0," + iH + ")")
        .call(d3.axisBottom(x)).selectAll("text").attr("font-size", 10);
      g.append("g").call(d3.axisLeft(y).ticks(5));

      svg.append("text").attr("x", W / 2).attr("y", H - 4)
        .attr("text-anchor", "middle").attr("font-size", 12).attr("fill", "#5A5246")
        .text("Extraction coverage (% of fields populated)");
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 18. Author Network
  // ────────────────────────────────────────────────────────────────

  /**
   * Bipartite force graph linking author nodes (amber) to domain
   * nodes (teal).  Node radius is proportional to page count.
   * Data comes from the `authorship` endpoint's `author_network`.
   */
  VIZ.authornetwork = function () {
    fetchJSON(API.authorship).then(function (data) {
      hideLoading("loading-authornetwork");
      var net = data.author_network || { nodes: [], links: [] };
      if (!net.nodes.length) {
        d3.select("#viz-authornetwork").append("p").attr("class", "viz-desc")
          .style("text-align", "center").style("padding", "60px 0")
          .text("No author data detected. The author field was empty for all pages.");
        return;
      }

      var sz = vizSize("viz-authornetwork");
      var W = sz.w, H = Math.max(500, sz.h);
      var maxPages = d3.max(net.nodes, function (n) { return n.pages; }) || 1;
      var rScale = d3.scaleSqrt().domain([0, maxPages]).range([4, 35]);

      var svg = d3.select("#viz-authornetwork").append("svg").attr("width", W).attr("height", H);
      var g = svg.append("g");
      svg.call(d3.zoom().scaleExtent([0.2, 5]).on("zoom", function (evt) {
        g.attr("transform", evt.transform);
      }));

      var authorColour = "#C4841D", domainColour = "#2D6A4F";
      var maxWeight = d3.max(net.links, function (l) { return l.weight; }) || 1;

      var sim = d3.forceSimulation(net.nodes)
        .force("link", d3.forceLink(net.links).id(function (d) { return d.id; }).distance(100).strength(0.3))
        .force("charge", d3.forceManyBody().strength(-180))
        .force("center", d3.forceCenter(W / 2, H / 2))
        .force("collision", d3.forceCollide().radius(function (d) { return rScale(d.pages) + 3; }));

      var link = g.selectAll("line").data(net.links).enter().append("line")
        .attr("stroke", "#ccc").attr("stroke-opacity", 0.5)
        .attr("stroke-width", function (d) { return Math.max(0.5, d.weight / maxWeight * 4); });

      var node = g.selectAll("circle").data(net.nodes).enter().append("circle")
        .attr("r", function (d) { return rScale(d.pages); })
        .attr("fill", function (d) { return d.type === "author" ? authorColour : domainColour; })
        .attr("stroke", "#fff").attr("stroke-width", 1.5).attr("cursor", "pointer")
        .on("mouseover", function (evt, d) {
          showTip(evt, "<strong>" + d.label + "</strong><br>" +
            (d.type === "author" ? "Author" : "Domain") + "<br>Pages: " + fmt(d.pages));
        })
        .on("mouseout", hideTip)
        .call(d3.drag()
          .on("start", function (evt, d) { if (!evt.active) sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
          .on("drag", function (evt, d) { d.fx = evt.x; d.fy = evt.y; })
          .on("end", function (evt, d) { if (!evt.active) sim.alphaTarget(0); d.fx = null; d.fy = null; })
        );

      var labels = g.selectAll("text").data(net.nodes.filter(function (n) { return n.pages > 5; }))
        .enter().append("text")
        .text(function (d) { return d.type === "domain" ? shortDomain(d.label) : d.label; })
        .attr("font-size", 9).attr("fill", "#1A1A1A").attr("text-anchor", "middle")
        .attr("dy", function (d) { return rScale(d.pages) + 12; })
        .attr("pointer-events", "none");

      sim.on("tick", function () {
        link.attr("x1", function (d) { return d.source.x; }).attr("y1", function (d) { return d.source.y; })
          .attr("x2", function (d) { return d.target.x; }).attr("y2", function (d) { return d.target.y; });
        node.attr("cx", function (d) { return d.x; }).attr("cy", function (d) { return d.y; });
        labels.attr("x", function (d) { return d.x; }).attr("y", function (d) { return d.y; });
      });

      buildLegend("legend-authornetwork", [
        { label: "Author", colour: authorColour },
        { label: "Domain", colour: domainColour }
      ]);
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 19. Publisher Landscape
  // ────────────────────────────────────────────────────────────────

  /**
   * Simple horizontal bar chart of publisher names ranked by total
   * page count, with domain-count annotation.  Data sourced from the
   * `authorship` endpoint's `publishers` array.
   */
  VIZ.publishers = function () {
    fetchJSON(API.authorship).then(function (data) {
      hideLoading("loading-publishers");
      var pubs = (data.publishers || []).slice(0, 30);
      if (!pubs.length) {
        d3.select("#viz-publishers").append("p").attr("class", "viz-desc")
          .style("text-align", "center").style("padding", "60px 0")
          .text("No publisher data detected.");
        return;
      }

      var margin = { top: 10, right: 60, bottom: 10, left: 200 };
      var barH = 22, gap = 4;
      var sz = vizSize("viz-publishers");
      var W = sz.w;
      var iW = W - margin.left - margin.right;
      var H = margin.top + margin.bottom + pubs.length * (barH + gap);
      var maxPages = d3.max(pubs, function (d) { return d.total_pages; }) || 1;
      var x = d3.scaleLinear().domain([0, maxPages]).range([0, iW]);

      var svg = d3.select("#viz-publishers").append("svg").attr("width", W).attr("height", H);
      var g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      pubs.forEach(function (pub, i) {
        var y = i * (barH + gap);
        var label = pub.publisher.length > 28 ? pub.publisher.slice(0, 26) + "\u2026" : pub.publisher;
        g.append("text").attr("x", -6).attr("y", y + barH / 2 + 4)
          .attr("text-anchor", "end").attr("font-size", 11).attr("fill", "#1A1A1A").text(label);
        g.append("rect").attr("x", 0).attr("y", y)
          .attr("width", x(pub.total_pages)).attr("height", barH)
          .attr("fill", "#2D6A4F").attr("fill-opacity", 0.8).attr("rx", 2)
          .on("mouseover", function (evt) {
            showTip(evt, "<strong>" + pub.publisher + "</strong><br>" +
              fmt(pub.total_pages) + " pages across " + pub.domain_count + " domains");
          })
          .on("mouseout", hideTip);
        g.append("text").attr("x", x(pub.total_pages) + 4).attr("y", y + barH / 2 + 4)
          .attr("font-size", 10).attr("fill", "#5A5246")
          .text(fmt(pub.total_pages) + " (" + pub.domain_count + " domains)");
      });
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 20. Schema Insights (conditional)
  // ────────────────────────────────────────────────────────────────

  /**
   * Conditionally rendered panel that appears only when the crawl
   * detected domain-specific Schema.org types (Product, Event,
   * JobPosting, Recipe).  Each detected type gets its own sub-section
   * with summary stats and a small inline chart (scatter, timeline,
   * or bar chart).  If none are found, a "no data" message is shown.
   */
  VIZ.schemainsights = function () {
    fetchJSON(API.schema_insights).then(function (data) {
      hideLoading("loading-schemainsights");
      var container = d3.select("#viz-schemainsights");
      var hasAny = false;

      // ── Products section with price histogram + rating scatter ──
      if (data.products) {
        hasAny = true;
        var sec = container.append("div").style("margin-bottom", "32px");
        sec.append("h3").style("font-size", "15px").style("font-weight", "700")
          .style("color", "var(--csj-headline)").style("margin", "0 0 8px")
          .text("Products (" + fmt(data.products.count) + " items)");
        var statsHtml = [
          "Price range: " + data.products.price_min + " \u2013 " + data.products.price_max,
          "Average price: " + data.products.price_avg,
          "Domains: " + data.products.by_domain.length
        ].join(" \u00B7 ");
        sec.append("p").style("font-size", "13px").style("color", "var(--csj-body)")
          .style("margin", "0 0 12px").html(statsHtml);

        var avail = data.products.availability;
        if (avail && Object.keys(avail).length) {
          sec.append("div").style("margin-bottom", "12px").html(
            Object.keys(avail).map(function (k) {
              return '<span class="ndo-badge" style="margin-right:4px;">' + k + ': ' + avail[k] + '</span>';
            }).join("")
          );
        }

        // Price histogram
        var prices = (data.products.top_rated || []).concat(
          (data.products.by_domain || []).length > 0 ? [] : []
        );
        var allPrices = [];
        if (data.products.price_min > 0 && data.products.price_max > 0) {
          var pRange = data.products.price_max - data.products.price_min;
          var bucketSize = Math.max(1, Math.ceil(pRange / 10));
          var pBuckets = {};
          var byDom = data.products.by_domain || [];
          sec.append("div").style("margin-bottom", "12px").html(
            "<strong>By domain:</strong> " + byDom.slice(0, 8).map(function (d) {
              return '<span class="ndo-badge" style="margin-right:4px;">' + shortDomain(d.domain) + ': ' + d.count + '</span>';
            }).join("")
          );
        }

        // Rating scatter (if top_rated has data)
        var rated = (data.products.top_rated || []).filter(function (p) { return p.rating > 0; });
        if (rated.length >= 3) {
          var scW = 400, scH = 200;
          var scM = { top: 10, right: 20, bottom: 30, left: 40 };
          var scIW = scW - scM.left - scM.right, scIH = scH - scM.top - scM.bottom;
          var scSvg = sec.append("svg").attr("width", scW).attr("height", scH);
          var scG = scSvg.append("g").attr("transform", "translate(" + scM.left + "," + scM.top + ")");
          var scX = d3.scaleLinear().domain([0, d3.max(rated, function (p) { return p.review_count; }) || 1]).range([0, scIW]);
          var scY = d3.scaleLinear().domain([0, 5]).range([scIH, 0]);
          scG.append("g").attr("transform", "translate(0," + scIH + ")").call(d3.axisBottom(scX).ticks(5));
          scG.append("g").call(d3.axisLeft(scY).ticks(5));
          scG.selectAll("circle").data(rated).enter().append("circle")
            .attr("cx", function (d) { return scX(d.review_count); })
            .attr("cy", function (d) { return scY(d.rating); })
            .attr("r", 5).attr("fill", "#C4841D").attr("fill-opacity", 0.7)
            .attr("stroke", "#fff").attr("stroke-width", 1)
            .on("mouseover", function (evt, d) {
              showTip(evt, "<strong>" + d.title + "</strong><br>Rating: " + d.rating +
                "<br>Reviews: " + fmt(d.review_count) + "<br>Price: " + d.price);
            })
            .on("mouseout", hideTip);
          scSvg.append("text").attr("x", scW / 2).attr("y", scH - 2)
            .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", "#5A5246").text("Review count");
          scSvg.append("text").attr("transform", "rotate(-90)")
            .attr("x", -scH / 2).attr("y", 12)
            .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", "#5A5246").text("Rating");
        }
      }

      // ── Events section with timeline ──
      if (data.events) {
        hasAny = true;
        var sec2 = container.append("div").style("margin-bottom", "32px");
        sec2.append("h3").style("font-size", "15px").style("font-weight", "700")
          .style("color", "var(--csj-headline)").style("margin", "0 0 8px")
          .text("Events (" + fmt(data.events.count) + " items)");

        // Event timeline bars
        var evts = (data.events.events || []).slice(0, 30).filter(function (e) { return e.date; });
        if (evts.length >= 2) {
          var evW = 600, evH = Math.max(180, evts.length * 18 + 40);
          var evM = { top: 20, right: 20, bottom: 10, left: 200 };
          var evIW = evW - evM.left - evM.right;
          var evDates = evts.map(function (e) { return new Date(e.date); }).filter(function (d) { return !isNaN(d); });
          if (evDates.length >= 2) {
            var evX = d3.scaleTime().domain(d3.extent(evDates)).range([0, evIW]);
            var evSvg = sec2.append("svg").attr("width", evW).attr("height", evH);
            var evG = evSvg.append("g").attr("transform", "translate(" + evM.left + "," + evM.top + ")");
            evG.append("g").call(d3.axisTop(evX).ticks(6).tickFormat(d3.timeFormat("%b %Y")))
              .selectAll("text").attr("font-size", 9);
            evts.forEach(function (ev, i) {
              var y = i * 18;
              var evDate = new Date(ev.date);
              if (isNaN(evDate)) return;
              evSvg.append("text").attr("x", evM.left - 4).attr("y", evM.top + y + 10)
                .attr("text-anchor", "end").attr("font-size", 9).attr("fill", "#1A1A1A")
                .text((ev.title || "").slice(0, 28));
              evG.append("circle").attr("cx", evX(evDate)).attr("cy", y + 8).attr("r", 5)
                .attr("fill", "#2D6A4F").attr("fill-opacity", 0.8)
                .on("mouseover", function (evt) {
                  showTip(evt, "<strong>" + ev.title + "</strong><br>" + ev.date +
                    (ev.location ? "<br>" + ev.location : ""));
                })
                .on("mouseout", hideTip);
            });
          }
        } else {
          sec2.append("div").html(evts.map(function (ev) {
            return '<div style="padding:3px 0;font-size:12px;border-bottom:1px solid var(--csj-newsprint);">' +
              '<strong>' + ev.date + '</strong> \u2014 ' + ev.title +
              (ev.location ? ' <span style="color:var(--csj-body);">(' + ev.location + ')</span>' : '') + '</div>';
          }).join(""));
        }
      }

      // ── Jobs section with location bar chart ──
      if (data.jobs) {
        hasAny = true;
        var sec3 = container.append("div").style("margin-bottom", "32px");
        sec3.append("h3").style("font-size", "15px").style("font-weight", "700")
          .style("color", "var(--csj-headline)").style("margin", "0 0 8px")
          .text("Job Postings (" + fmt(data.jobs.count) + " items)");

        var locs = (data.jobs.by_location || []).slice(0, 12);
        if (locs.length >= 2) {
          var jW = 450, jBarH = 20, jGap = 3;
          var jM = { top: 5, right: 50, bottom: 5, left: 150 };
          var jIW = jW - jM.left - jM.right;
          var jH = jM.top + jM.bottom + locs.length * (jBarH + jGap);
          var jMax = d3.max(locs, function (d) { return d.count; }) || 1;
          var jX = d3.scaleLinear().domain([0, jMax]).range([0, jIW]);
          var jSvg = sec3.append("svg").attr("width", jW).attr("height", jH);
          var jG = jSvg.append("g").attr("transform", "translate(" + jM.left + "," + jM.top + ")");
          locs.forEach(function (loc, i) {
            var y = i * (jBarH + jGap);
            jG.append("text").attr("x", -4).attr("y", y + jBarH / 2 + 4)
              .attr("text-anchor", "end").attr("font-size", 10).attr("fill", "#1A1A1A")
              .text(loc.location.length > 22 ? loc.location.slice(0, 20) + "\u2026" : loc.location);
            jG.append("rect").attr("x", 0).attr("y", y).attr("width", jX(loc.count))
              .attr("height", jBarH).attr("fill", "#3A6B7E").attr("fill-opacity", 0.8).attr("rx", 2);
            jG.append("text").attr("x", jX(loc.count) + 4).attr("y", y + jBarH / 2 + 4)
              .attr("font-size", 10).attr("fill", "#5A5246").text(loc.count);
          });
        } else if (locs.length) {
          sec3.append("div").html(locs.map(function (loc) {
            return '<span class="ndo-badge" style="margin-right:4px;">' + loc.location + ': ' + loc.count + '</span>';
          }).join(""));
        }
      }

      // ── Recipes section ──
      if (data.recipes) {
        hasAny = true;
        var sec4 = container.append("div").style("margin-bottom", "32px");
        sec4.append("h3").style("font-size", "15px").style("font-weight", "700")
          .style("color", "var(--csj-headline)").style("margin", "0 0 8px")
          .text("Recipes (" + fmt(data.recipes.count) + " items)");
        var recDoms = (data.recipes.by_domain || []).slice(0, 8);
        if (recDoms.length) {
          sec4.append("div").html(
            "<strong>By domain:</strong> " + recDoms.map(function (d) {
              return '<span class="ndo-badge" style="margin-right:4px;">' + shortDomain(d.domain) + ': ' + d.count + '</span>';
            }).join("")
          );
        }
      }

      if (!hasAny) {
        container.append("p").attr("class", "viz-desc")
          .style("text-align", "center").style("padding", "60px 0")
          .text("No domain-specific schema data (Product, Event, Job, Recipe) found in this crawl. These insights appear automatically when relevant Schema.org types are detected.");
      }
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 21. Link Flow (Sankey Diagram)
  // ────────────────────────────────────────────────────────────────

  VIZ.linkflow = function () {
    var topN = parseInt(document.getElementById("linkflow-top").value, 10) || 15;
    var url = API.link_flow + (API.link_flow.indexOf("?") > -1 ? "&" : "?") + "top=" + topN;
    fetchJSON(url).then(function (data) {
      hideLoading("loading-linkflow");
      var container = d3.select("#viz-linkflow");

      if (!data.nodes || !data.nodes.length || !data.links || !data.links.length) {
        container.append("p").attr("class", "viz-desc")
          .style("text-align", "center").style("padding", "60px 0")
          .text("No cross-domain link data available for a Sankey diagram.");
        return;
      }

      _assignOwnerColours(data.nodes);

      var sz = vizSize("viz-linkflow");
      var margin = { top: 10, right: 10, bottom: 10, left: 10 };
      var W = sz.w - margin.left - margin.right;
      var H = Math.max(500, data.nodes.length * 30);

      var svg = container.append("svg")
        .attr("width", W + margin.left + margin.right)
        .attr("height", H + margin.top + margin.bottom)
        .append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      var sankey = d3.sankey()
        .nodeId(function (d) { return d.index; })
        .nodeWidth(18)
        .nodePadding(12)
        .nodeSort(null)
        .extent([[0, 0], [W, H]]);

      var graph = sankey({
        nodes: data.nodes.map(function (d, i) { return Object.assign({}, d, { index: i }); }),
        links: data.links.map(function (d) { return Object.assign({}, d); })
      });

      svg.append("g")
        .selectAll("rect")
        .data(graph.nodes)
        .join("rect")
        .attr("x", function (d) { return d.x0; })
        .attr("y", function (d) { return d.y0; })
        .attr("height", function (d) { return Math.max(1, d.y1 - d.y0); })
        .attr("width", function (d) { return d.x1 - d.x0; })
        .attr("fill", function (d) { return ownerColour(d.ownership); })
        .attr("stroke", "#333")
        .attr("stroke-width", 0.5)
        .on("mouseover", function (evt, d) {
          showTip(evt, "<strong>" + shortDomain(d.name) + "</strong><br>" +
            fmt(d.pages) + " pages<br>Owner: " + d.ownership);
        })
        .on("mouseout", hideTip);

      svg.append("g")
        .attr("fill", "none")
        .selectAll("path")
        .data(graph.links)
        .join("path")
        .attr("d", d3.sankeyLinkHorizontal())
        .attr("stroke", function (d) { return ownerColour(d.source.ownership); })
        .attr("stroke-opacity", 0.35)
        .attr("stroke-width", function (d) { return Math.max(1, d.width); })
        .on("mouseover", function (evt, d) {
          d3.select(this).attr("stroke-opacity", 0.65);
          showTip(evt, shortDomain(d.source.name) + " → " + shortDomain(d.target.name) +
            "<br><strong>" + fmt(d.value) + "</strong> links");
        })
        .on("mouseout", function () {
          d3.select(this).attr("stroke-opacity", 0.35);
          hideTip();
        });

      svg.append("g")
        .selectAll("text")
        .data(graph.nodes)
        .join("text")
        .attr("x", function (d) { return d.x0 < W / 2 ? d.x1 + 6 : d.x0 - 6; })
        .attr("y", function (d) { return (d.y0 + d.y1) / 2; })
        .attr("dy", "0.35em")
        .attr("text-anchor", function (d) { return d.x0 < W / 2 ? "start" : "end"; })
        .attr("font-size", 11)
        .attr("fill", "#1A1A1A")
        .text(function (d) { return shortDomain(d.name); });

      var groups = {};
      graph.nodes.forEach(function (n) { groups[n.ownership] = ownerColour(n.ownership); });
      buildLegend("legend-linkflow", Object.keys(groups).map(function (g) {
        return { label: g, colour: groups[g] };
      }));
    });
  };


  // ────────────────────────────────────────────────────────────────
  // 22. Page Depth Analysis
  // ────────────────────────────────────────────────────────────────

  VIZ.pagedepth = function () {
    fetchJSON(API.page_depth).then(function (data) {
      hideLoading("loading-pagedepth");
      var container = d3.select("#viz-pagedepth");

      var hist = data.depth_histogram || [];
      var quality = data.depth_quality || [];

      if (!hist.length) {
        container.append("p").attr("class", "viz-desc")
          .style("text-align", "center").style("padding", "60px 0")
          .text("No page depth data available.");
        return;
      }

      var sz = vizSize("viz-pagedepth");
      var margin = { top: 30, right: 60, bottom: 50, left: 60 };
      var W = sz.w - margin.left - margin.right;
      var H = sz.h - margin.top - margin.bottom;

      var svg = container.append("svg")
        .attr("width", sz.w)
        .attr("height", sz.h)
        .append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      var xDomain = hist.map(function (d) { return d.depth; });
      var x = d3.scaleBand().domain(xDomain).range([0, W]).padding(0.2);
      var yMax = d3.max(hist, function (d) { return d.count; }) || 1;
      var y = d3.scaleLinear().domain([0, yMax * 1.1]).range([H, 0]);

      svg.append("g").attr("transform", "translate(0," + H + ")")
        .call(d3.axisBottom(x).tickFormat(function (d) { return "Depth " + d; }))
        .selectAll("text").attr("font-size", 10);

      svg.append("g").call(d3.axisLeft(y).ticks(6).tickFormat(fmt))
        .selectAll("text").attr("font-size", 10);

      svg.append("text").attr("x", W / 2).attr("y", H + 40).attr("text-anchor", "middle")
        .attr("font-size", 12).attr("fill", "#5A5246").text("Crawl Depth");
      svg.append("text").attr("transform", "rotate(-90)").attr("x", -H / 2).attr("y", -45)
        .attr("text-anchor", "middle").attr("font-size", 12).attr("fill", "#5A5246").text("Pages");

      svg.selectAll(".depth-bar")
        .data(hist)
        .join("rect")
        .attr("class", "depth-bar")
        .attr("x", function (d) { return x(d.depth); })
        .attr("y", function (d) { return y(d.count); })
        .attr("width", x.bandwidth())
        .attr("height", function (d) { return H - y(d.count); })
        .attr("fill", "#2D6A4F")
        .attr("fill-opacity", 0.75)
        .attr("rx", 2)
        .on("mouseover", function (evt, d) {
          d3.select(this).attr("fill-opacity", 1);
          var q = quality.find(function (q) { return q.depth === d.depth; });
          var tipHtml = "<strong>Depth " + d.depth + "</strong><br>" +
            fmt(d.count) + " pages";
          if (q) {
            tipHtml += "<br>Avg words: " + fmt(q.avg_words) +
              "<br>Avg coverage: " + q.avg_coverage + "%" +
              "<br>Avg readability: " + q.avg_readability;
          }
          showTip(evt, tipHtml);
        })
        .on("mouseout", function () {
          d3.select(this).attr("fill-opacity", 0.75);
          hideTip();
        });

      function drawOverlay(metric, colour, label) {
        svg.selectAll(".depth-overlay").remove();
        svg.selectAll(".depth-overlay-label").remove();

        if (!quality.length) return;

        var oMax = d3.max(quality, function (d) { return d[metric]; }) || 1;
        var y2 = d3.scaleLinear().domain([0, oMax * 1.15]).range([H, 0]);

        svg.selectAll(".axis-right").remove();
        svg.append("g").attr("class", "axis-right")
          .attr("transform", "translate(" + W + ",0)")
          .call(d3.axisRight(y2).ticks(5))
          .selectAll("text").attr("font-size", 10);

        var lineGen = d3.line()
          .x(function (d) { return x(d.depth) + x.bandwidth() / 2; })
          .y(function (d) { return y2(d[metric]); })
          .curve(d3.curveMonotoneX);

        svg.append("path").datum(quality)
          .attr("class", "depth-overlay")
          .attr("fill", "none")
          .attr("stroke", colour)
          .attr("stroke-width", 2.5)
          .attr("d", lineGen);

        svg.selectAll(".depth-overlay-dot")
          .data(quality)
          .join("circle")
          .attr("class", "depth-overlay")
          .attr("cx", function (d) { return x(d.depth) + x.bandwidth() / 2; })
          .attr("cy", function (d) { return y2(d[metric]); })
          .attr("r", 4)
          .attr("fill", colour)
          .attr("stroke", "#fff")
          .attr("stroke-width", 1.5);

        svg.append("text").attr("class", "depth-overlay-label")
          .attr("x", W + 8).attr("y", y2(quality[quality.length - 1][metric]) - 8)
          .attr("font-size", 10).attr("fill", colour).text(label);
      }

      var overlaySelect = document.getElementById("pagedepth-overlay");
      var overlayMap = {
        words: { metric: "avg_words", colour: "#C4841D", label: "Avg Words" },
        coverage: { metric: "avg_coverage", colour: "#1565c0", label: "Avg Coverage %" },
        readability: { metric: "avg_readability", colour: "#A4243B", label: "Avg Readability" }
      };

      function applyOverlay() {
        var sel = overlayMap[overlaySelect.value] || overlayMap.words;
        drawOverlay(sel.metric, sel.colour, sel.label);
      }
      applyOverlay();

      overlaySelect.addEventListener("change", function () {
        applyOverlay();
      });

      buildLegend("legend-pagedepth", [
        { label: "Page count (bars)", colour: "#2D6A4F" },
        { label: "Quality overlay (line)", colour: "#C4841D" }
      ]);
    });
  };


  // ────────────────────────────────────────────────────────────────
  // 23. Content Health Matrix (Heatmap)
  // ────────────────────────────────────────────────────────────────

  VIZ.contenthealth = function () {
    fetchJSON(API.content_health).then(function (data) {
      hideLoading("loading-contenthealth");
      var container = d3.select("#viz-contenthealth");

      if (!data.domains || !data.domains.length) {
        container.append("p").attr("class", "viz-desc")
          .style("text-align", "center").style("padding", "60px 0")
          .text("No content health data available.");
        return;
      }

      var domains = data.domains;
      var signals = data.signals;
      var matrix = data.matrix;
      var pageCounts = data.page_counts;

      var cellW = 52, cellH = 22;
      var labelW = 180, headerH = 90;
      var countColW = 55;
      var margin = { top: 8, right: 30, bottom: 20, left: 8 };
      var gridW = signals.length * cellW;
      var gridH = domains.length * cellH;
      var totalW = margin.left + labelW + gridW + countColW + margin.right;
      var totalH = margin.top + headerH + gridH + margin.bottom;

      var colour = d3.scaleSequential(d3.interpolateRdYlGn).domain([0, 100]);

      var svg = container.append("svg")
        .attr("width", totalW)
        .attr("height", totalH);

      var g = svg.append("g")
        .attr("transform", "translate(" + (margin.left + labelW) + "," + (margin.top + headerH) + ")");

      signals.forEach(function (sig, si) {
        svg.append("text")
          .attr("x", margin.left + labelW + si * cellW + cellW / 2)
          .attr("y", margin.top + headerH - 6)
          .attr("text-anchor", "end")
          .attr("transform", "rotate(-50," + (margin.left + labelW + si * cellW + cellW / 2) + "," + (margin.top + headerH - 6) + ")")
          .attr("font-size", 10)
          .attr("font-weight", 600)
          .attr("fill", "#1A1A1A")
          .text(sig);
      });

      domains.forEach(function (dom, di) {
        svg.append("text")
          .attr("x", margin.left + labelW - 6)
          .attr("y", margin.top + headerH + di * cellH + cellH / 2 + 4)
          .attr("text-anchor", "end")
          .attr("font-size", 10)
          .attr("fill", "#1A1A1A")
          .text(shortDomain(dom).length > 24 ? shortDomain(dom).slice(0, 22) + "\u2026" : shortDomain(dom));

        svg.append("text")
          .attr("x", margin.left + labelW + gridW + 6)
          .attr("y", margin.top + headerH + di * cellH + cellH / 2 + 4)
          .attr("text-anchor", "start")
          .attr("font-size", 9)
          .attr("fill", "#5A5246")
          .text(fmt(pageCounts[di]) + "p");

        signals.forEach(function (sig, si) {
          var val = matrix[di][si];
          g.append("rect")
            .attr("x", si * cellW + 1)
            .attr("y", di * cellH + 1)
            .attr("width", cellW - 2)
            .attr("height", cellH - 2)
            .attr("rx", 2)
            .attr("fill", colour(val))
            .attr("stroke", "#fff")
            .attr("stroke-width", 1)
            .on("mouseover", function (evt) {
              showTip(evt, "<strong>" + shortDomain(dom) + "</strong><br>" +
                sig + ": <strong>" + val + "%</strong><br>" +
                fmt(pageCounts[di]) + " pages");
            })
            .on("mouseout", hideTip);

          if (cellW > 30) {
            g.append("text")
              .attr("x", si * cellW + cellW / 2)
              .attr("y", di * cellH + cellH / 2 + 3)
              .attr("text-anchor", "middle")
              .attr("font-size", 9)
              .attr("font-weight", 600)
              .attr("fill", val > 65 ? "#fff" : (val < 35 ? "#fff" : "#1A1A1A"))
              .attr("pointer-events", "none")
              .text(Math.round(val));
          }
        });
      });

      var legendW = 200, legendH = 12;
      var legendG = svg.append("g")
        .attr("transform", "translate(" + (margin.left + labelW) + "," + (totalH - margin.bottom + 4) + ")");

      var defs = svg.append("defs");
      var grad = defs.append("linearGradient").attr("id", "health-grad");
      grad.append("stop").attr("offset", "0%").attr("stop-color", colour(0));
      grad.append("stop").attr("offset", "50%").attr("stop-color", colour(50));
      grad.append("stop").attr("offset", "100%").attr("stop-color", colour(100));

      legendG.append("rect").attr("width", legendW).attr("height", legendH).attr("rx", 2)
        .attr("fill", "url(#health-grad)");
      legendG.append("text").attr("x", 0).attr("y", legendH + 11).attr("font-size", 9)
        .attr("fill", "#5A5246").text("0%");
      legendG.append("text").attr("x", legendW / 2).attr("y", legendH + 11)
        .attr("text-anchor", "middle").attr("font-size", 9).attr("fill", "#5A5246").text("50%");
      legendG.append("text").attr("x", legendW).attr("y", legendH + 11)
        .attr("text-anchor", "end").attr("font-size", 9).attr("fill", "#5A5246").text("100%");
    });
  };


  // ── One-time control listeners ──────────────────────────────────
  // Registered here (outside VIZ functions) so they are never duplicated,
  // regardless of how many times the corresponding VIZ function is called.
  document.getElementById("chord-top").addEventListener("change", function () {
    Object.keys(cache).forEach(function (k) {
      if (k.indexOf(API.chord) === 0) cache[k] = null;
    });
    rendered["chord"] = false;
    d3.select("#viz-chord").selectAll("*").remove();
    VIZ.chord();
  });

  document.getElementById("linkflow-top").addEventListener("change", function () {
    Object.keys(cache).forEach(function (k) {
      if (k.indexOf(API.link_flow) === 0) cache[k] = null;
    });
    rendered["linkflow"] = false;
    d3.select("#viz-linkflow").selectAll("*").remove();
    VIZ.linkflow();
  });

  // ── Initial render ──────────────────────────────────────────────
  // The "network" tab is active by default in the HTML, so trigger
  // its render immediately on page load.
  renderPanel("network");

})();
