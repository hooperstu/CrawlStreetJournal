/* ================================================================
   Ecosystem Dashboard — D3.js Visualisations
   ================================================================
   Lazy-loads data per tab. Each viz function receives its container
   selector and renders into it.  Shared helpers at the top.
   ================================================================ */
(function () {
  "use strict";

  var API = window.ECO_API;
  var cache = {};
  var rendered = {};

  // ── Colour palettes ─────────────────────────────────────────────
  var OWNER_COLOURS = {};
  var _ownerIndex = 0;
  var STATUS_COLOURS = { "2": "#2D6A4F", "3": "#C4841D", "4": "#B8860B", "5": "#A4243B" };
  var CSJ_PALETTE = [
    "#1A1A1A", "#C4841D", "#2D6A4F", "#A4243B", "#5A5246",
    "#7B6D53", "#3A6B7E", "#8B5E3C", "#6B4226", "#4A6741",
    "#9B7042", "#B07D3A", "#3D5A4C", "#7A4F5A", "#5C7A6B"
  ];

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
  var tip = d3.select("#vizTooltip");

  function showTip(evt, html) {
    tip.html(html).classed("visible", true);
    var tx = Math.min(evt.pageX + 14, window.innerWidth - 340);
    var ty = evt.pageY - 10;
    tip.style("left", tx + "px").style("top", ty + "px");
  }
  function hideTip() { tip.classed("visible", false); }

  // ── Helpers ─────────────────────────────────────────────────────
  function fmt(n) { return d3.format(",")(n); }
  function shortDomain(d) { return d.replace(/^www\./, ""); }
  function hideLoading(id) { var el = document.getElementById(id); if (el) el.classList.add("hidden"); }

  function fetchJSON(url) {
    if (cache[url]) return Promise.resolve(cache[url]);
    return d3.json(url).then(function (d) { cache[url] = d; return d; });
  }

  function ownerColour(o) { return OWNER_COLOURS[o] || "#5A5246"; }

  function buildLegend(id, items) {
    var el = document.getElementById(id);
    if (!el) return;
    el.innerHTML = items.map(function (it) {
      return '<span class="viz-legend-item"><span class="viz-legend-swatch" style="background:' +
        it.colour + ';"></span>' + it.label + '</span>';
    }).join("");
  }

  function vizSize(id) {
    var el = document.getElementById(id);
    var w = el ? el.clientWidth : 900;
    return { w: w, h: Math.max(500, Math.min(w * 0.65, 700)) };
  }

  // ── Tab controller ──────────────────────────────────────────────
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

  function renderPanel(name) {
    if (rendered[name]) return;
    rendered[name] = true;
    var fn = VIZ[name];
    if (fn) fn();
  }

  // ── Viz implementations ─────────────────────────────────────────
  var VIZ = {};

  // ────────────────────────────────────────────────────────────────
  // 1. Force-Directed Network Graph
  // ────────────────────────────────────────────────────────────────
  VIZ.network = function () {
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
      var adj = {};
      data.nodes.forEach(function (n) { adj[n.id] = new Set(); });
      data.links.forEach(function (l) {
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

      var g = svg.append("g");

      svg.call(d3.zoom().scaleExtent([0.2, 5]).on("zoom", function (evt) {
        g.attr("transform", evt.transform);
      }));

      var maxPages = d3.max(data.nodes, function (n) { return n.pages; }) || 1;
      var rScale = d3.scaleSqrt().domain([0, maxPages]).range([3, 40]);
      var maxWeight = d3.max(data.links, function (l) { return l.weight; }) || 1;

      var sim = d3.forceSimulation(data.nodes)
        .force("link", d3.forceLink(data.links).id(function (d) { return d.id; })
          .distance(function (d) { return 120 - Math.min(d.weight / maxWeight * 60, 50); })
          .strength(function (d) { return 0.2 + d.weight / maxWeight * 0.5; }))
        .force("charge", d3.forceManyBody().strength(-200))
        .force("center", d3.forceCenter(W / 2, H / 2))
        .force("collision", d3.forceCollide().radius(function (d) { return rScale(d.pages) + 2; }));

      var link = g.selectAll("line").data(data.links).enter().append("line")
        .attr("stroke", "#ccc")
        .attr("stroke-width", function (d) { return Math.max(0.5, Math.min(d.weight / maxWeight * 4, 6)); })
        .attr("stroke-opacity", 0.4);

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

      var labels = g.selectAll("text").data(data.nodes.filter(function (n) { return n.pages > 20; }))
        .enter().append("text")
        .text(function (d) { return shortDomain(d.id); })
        .attr("font-size", 10)
        .attr("fill", "#1A1A1A")
        .attr("text-anchor", "middle")
        .attr("dy", function (d) { return rScale(d.pages) + 12; })
        .attr("pointer-events", "none");

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
      function selectNode(d) {
        if (selectedNode && selectedNode.id === d.id) {
          clearSelection();
          return;
        }
        selectedNode = d;
        var primary = adj[d.id] || new Set();
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

      svg.on("click", function () { clearSelection(); });
      document.getElementById("ndo-close").addEventListener("click", function () { clearSelection(); });

      // ── Overlay population ───────────────────────────────────
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
      var CMS_COLOURS = {};
      var _cmsIdx = 0;
      domains.forEach(function (d) {
        var c = d.cms_generator || "(undetected)";
        if (!CMS_COLOURS[c]) { CMS_COLOURS[c] = CSJ_PALETTE[_cmsIdx % CSJ_PALETTE.length]; _cmsIdx++; }
      });
      var covScale = d3.scaleSequential(d3.interpolateViridis).domain([0, 100]);

      function nodeColour(d) {
        var mode = document.getElementById("network-colour-by").value;
        var detail = domainLookup[d.id] || {};
        if (mode === "cms") return CMS_COLOURS[detail.cms_generator || "(undetected)"] || "#5A5246";
        if (mode === "coverage") return covScale(detail.avg_extraction_coverage || 0);
        return ownerColour(d.ownership);
      }

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
  VIZ.treemap = function () {
    fetchJSON(API.domains).then(function (data) {
      hideLoading("loading-treemap");
      _assignOwnerColours(data);

      var TM_COLOURS = {};
      var _tmIdx = 0;
      function tmColour(key) {
        if (!TM_COLOURS[key]) { TM_COLOURS[key] = CSJ_PALETTE[_tmIdx % CSJ_PALETTE.length]; _tmIdx++; }
        return TM_COLOURS[key];
      }

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

        d3.treemap().size([W, H]).padding(2).paddingTop(18).round(true)(root);
        var svg = d3.select("#viz-treemap").append("svg").attr("width", W).attr("height", H);

        var groups = svg.selectAll("g").data(root.children).enter().append("g");
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

        var leaves = svg.selectAll(".leaf").data(root.leaves()).enter().append("g").attr("class", "leaf");
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
  VIZ.status = function () {
    fetchJSON(API.domains).then(function (data) {
      hideLoading("loading-status");
      var top = data.slice(0, 40);
      var margin = { top: 10, right: 20, bottom: 10, left: 200 };
      var barH = 20, gap = 3;
      var sz = vizSize("viz-status");
      var W = sz.w;
      var innerW = W - margin.left - margin.right;
      var H = margin.top + margin.bottom + top.length * (barH + gap);

      var svg = d3.select("#viz-status").append("svg").attr("width", W).attr("height", H);
      var g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      var maxPages = d3.max(top, function (d) { return d.page_count; }) || 1;
      var x = d3.scaleLinear().domain([0, maxPages]).range([0, innerW]);
      var categories = ["2", "3", "4", "5", "?"];
      var catColours = { "2": "#2D6A4F", "3": "#C4841D", "4": "#B8860B", "5": "#A4243B", "?": "#5A5246" };

      function sortData(mode) {
        if (mode === "errors") {
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
      sortData("errors");

      top.forEach(function (d, i) {
        var y = i * (barH + gap);
        g.append("text")
          .attr("x", -4).attr("y", y + barH / 2 + 4)
          .attr("text-anchor", "end").attr("font-size", 11).attr("fill", "#1A1A1A")
          .text(shortDomain(d.domain));

        var cumX = 0;
        var sc = d.status_codes || {};
        categories.forEach(function (cat) {
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

      document.getElementById("status-sort").addEventListener("change", function () {
        rendered["status"] = false;
        d3.select("#viz-status").selectAll("*").remove();
        VIZ.status();
      });
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 4. Analytics & Governance Matrix Heatmap
  // ────────────────────────────────────────────────────────────────
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

      var sz = vizSize("viz-analytics");
      var containerW = sz.w;

      var labelW = 210;
      var summaryColExtra = 50;
      var marginRight = 20 + summaryColExtra;
      var availableGridW = containerW - labelW - marginRight;
      var cellSize = Math.max(30, Math.min(48, Math.floor(availableGridW / Math.max(signals.length, 1))));
      var rowH = Math.max(cellSize, 28);

      var topLabelH = 160;
      var summaryRowH = rowH;
      var margin = { top: topLabelH, right: marginRight, bottom: 10 + summaryRowH + 8, left: labelW };
      var gridW = signals.length * cellSize;
      var gridH = top.length * rowH;
      var W = Math.max(containerW, margin.left + gridW + margin.right);
      var H = margin.top + gridH + margin.bottom;

      var colourScale = d3.scaleSequential(d3.interpolateBlues).domain([0, 1]);

      function signalLabel(sig) {
        if (sig === "privacy_policy") return "Privacy Policy";
        var label = sig.replace(/_/g, " ");
        var maxLen = Math.max(12, Math.floor(cellSize / 4.5));
        return label.length > maxLen ? label.substring(0, maxLen - 1) + "\u2026" : label;
      }
      function signalFullLabel(sig) {
        return sig === "privacy_policy" ? "Privacy Policy" : sig.replace(/_/g, " ");
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

      var svg = d3.select("#viz-analytics").append("svg").attr("width", W).attr("height", H);
      var g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      var colFontSize = Math.max(11, Math.min(13, cellSize * 0.38));
      signals.forEach(function (sig, j) {
        svg.append("text")
          .attr("x", 0).attr("y", 0)
          .attr("transform",
            "translate(" + (margin.left + j * cellSize + cellSize / 2) + "," + (margin.top - 10) + ") rotate(-55)")
          .attr("font-size", colFontSize).attr("text-anchor", "end").attr("fill", "#4c6272")
          .text(signalLabel(sig))
          .on("mouseover", function (evt) { showTip(evt, "<strong>" + signalFullLabel(sig) + "</strong>"); })
          .on("mouseout", hideTip);
      });

      var signalAdoption = signals.map(function () { return 0; });

      var rowFontSize = Math.max(11, Math.min(13, rowH * 0.45));
      top.forEach(function (d, i) {
        var domainLabel = shortDomain(d.domain);
        var maxDomainLen = Math.floor(labelW / 7);
        if (domainLabel.length > maxDomainLen) domainLabel = domainLabel.substring(0, maxDomainLen - 1) + "\u2026";

        g.append("text")
          .attr("x", -10).attr("y", i * rowH + rowH / 2)
          .attr("text-anchor", "end")
          .attr("dominant-baseline", "central")
          .attr("font-size", rowFontSize).attr("fill", "#1A1A1A")
          .text(domainLabel)
          .on("mouseover", function (evt) {
            showTip(evt, "<strong>" + shortDomain(d.domain) + "</strong><br>" +
              fmt(d.page_count) + " pages &middot; " + d._signalCount + " signal" + (d._signalCount !== 1 ? "s" : ""));
          })
          .on("mouseout", hideTip);

        signals.forEach(function (sig, j) {
          var present = isPresent(d, sig);
          var cov = coverage(d, sig);

          if (present) signalAdoption[j]++;

          g.append("rect")
            .attr("x", j * cellSize + 1).attr("y", i * rowH + 1)
            .attr("width", cellSize - 2).attr("height", rowH - 2)
            .attr("rx", 3)
            .attr("fill", present ? colourScale(Math.max(cov, 0.15)) : "#F2EFEB")
            .attr("stroke", present ? "rgba(196,132,29,.25)" : "none")
            .attr("stroke-width", 0.5)
            .on("mouseover", function (evt) {
              showTip(evt,
                "<strong>" + shortDomain(d.domain) + "</strong><br>" +
                signalFullLabel(sig) + ": " + (present ? "Yes" : "No") +
                (present ? "<br>Coverage: " + Math.round(cov * 100) + "% of " + fmt(d.page_count) + " pages" : ""));
            })
            .on("mouseout", hideTip);
        });

        g.append("text")
          .attr("x", gridW + 14).attr("y", i * rowH + rowH / 2)
          .attr("text-anchor", "start")
          .attr("dominant-baseline", "central")
          .attr("font-size", Math.max(10, rowFontSize - 1)).attr("fill", "#5A5246")
          .text(d._signalCount);
      });

      g.append("text")
        .attr("x", gridW + 14).attr("y", -10)
        .attr("text-anchor", "start").attr("font-size", 10)
        .attr("fill", "#5A5246").attr("font-weight", 600)
        .text("Total");

      signals.forEach(function (sig, j) {
        g.append("text")
          .attr("x", j * cellSize + cellSize / 2).attr("y", gridH + summaryRowH - 4)
          .attr("text-anchor", "middle").attr("font-size", Math.max(10, colFontSize - 1))
          .attr("fill", "#5A5246")
          .text(signalAdoption[j]);
      });

      g.append("text")
        .attr("x", -10).attr("y", gridH + summaryRowH - 4)
        .attr("text-anchor", "end").attr("font-size", 10)
        .attr("fill", "#5A5246").attr("font-weight", 600)
        .text("Adoption");

      g.append("line")
        .attr("x1", 0).attr("y1", gridH + 4)
        .attr("x2", gridW).attr("y2", gridH + 4)
        .attr("stroke", "#d8dde0").attr("stroke-width", 1);

      buildLegend("legend-analytics", [
        { colour: colourScale(1), label: "High coverage (most pages)" },
        { colour: colourScale(0.4), label: "Partial coverage" },
        { colour: colourScale(0.15), label: "Low coverage (few pages)" },
        { colour: "#F2EFEB", label: "Not detected" }
      ]);
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 5. Freshness Timeline
  // ────────────────────────────────────────────────────────────────
  VIZ.freshness = function () {
    Promise.all([fetchJSON(API.freshness), fetchJSON(API.domains)]).then(function (results) {
      var data = results[0];
      var domainData = results[1];
      hideLoading("loading-freshness");

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
  VIZ.chord = function () {
    var topN = document.getElementById("chord-top").value || 20;
    fetchJSON(API.chord + "?top=" + topN).then(function (data) {
      hideLoading("loading-chord");
      var sz = vizSize("viz-chord");
      var W = Math.min(sz.w, 700), H = W;
      var outerR = W / 2 - 80, innerR = outerR - 20;

      d3.select("#viz-chord").selectAll("*").remove();
      var svg = d3.select("#viz-chord").append("svg")
        .attr("width", W).attr("height", H)
        .append("g").attr("transform", "translate(" + W / 2 + "," + H / 2 + ")");

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

    document.getElementById("chord-top").addEventListener("change", function () {
      rendered["chord"] = false;
      cache[API.chord + "?top=" + this.value] = null;
      VIZ.chord();
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 7. Sunburst — Navigation Structure  (zoomable)
  // ────────────────────────────────────────────────────────────────
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

    function sbColour(d) {
      if (!d.parent) return "#F2EFEB";
      var isExt = d.data.external || (d.parent && d.parent.data.external);
      var baseHue = isExt ? SB_EXTERNAL_HUE : SB_INTERNAL_HUE;

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
    function labelFits(d) {
      return (d.y1 - d.y0) > 28 && (d.x1 - d.x0) > 0.06;
    }
    function labelTransform(d, R) {
      var x = ((d.x0 + d.x1) / 2) * 180 / Math.PI;
      var y = (d.y0 + d.y1) / 2;
      return "rotate(" + (x - 90) + ") translate(" + y + ",0) rotate(" + (x < 180 ? 0 : 180) + ")";
    }

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

      /* Centre circle for click-to-zoom-out */
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

      updateBreadcrumbs(root, zoomTo);

      /* ---- Zoom ---- */
      function zoomTo(target) {
        if (!target) target = root;
        currentRoot = target;
        updateBreadcrumbs(target, zoomTo);
        setCentreText(
          target.depth === 0 ? shortDomain(tree.name) : target.data.name,
          target.value + " items"
        );

        var t0 = target.x0, t1 = target.x1, ty0 = target.y0;
        var xScale = 2 * Math.PI / (t1 - t0 || 1);
        var yScale = R / (R - ty0 || 1);

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

        centreCircle.attr("r", function () {
          return target.depth === 0 ? (root.y1 || R * 0.18) : target._target ? target._target.y0 || R * 0.18 : R * 0.18;
        });
      }
    }
  };

  // ────────────────────────────────────────────────────────────────
  // 8. Radar / Spider Chart — Quality
  // ────────────────────────────────────────────────────────────────
  VIZ.radar = function () {
    fetchJSON(API.domains).then(function (data) {
      hideLoading("loading-radar");

      var eligible = data.filter(function (d) { return d.page_count >= 10; }).slice(0, 40);
      if (!eligible.length) return;

      var maxWords = d3.max(eligible, function (d) { return d.avg_word_count; }) || 1;
      var maxLinks = d3.max(eligible, function (d) { return d.avg_internal_links; }) || 1;

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
          langAttr:      (d.wcag_lang_pct || 0) / 100,
          headingOrder:  (d.wcag_heading_order_pct || 0) / 100,
          pageTitle:     (d.wcag_title_pct || 0) / 100,
          formLabels:    (d.wcag_form_labels_pct != null ? d.wcag_form_labels_pct : 100) / 100,
          landmarks:     (d.wcag_landmarks_pct || 0) / 100,
          linkPurpose:   Math.max(0, 1 - (d.wcag_vague_link_pct || 0) / 100),
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
        { key: "langAttr",       label: "Language Declared", tip: function (d) { return (d.wcag_lang_pct || 0).toFixed(0) + "% of pages declare lang"; } },
        { key: "headingOrder",   label: "Heading Hierarchy", tip: function (d) { return (d.wcag_heading_order_pct || 0).toFixed(0) + "% of pages have valid heading order"; } },
        { key: "pageTitle",      label: "Page Titled",       tip: function (d) { return (d.wcag_title_pct || 0).toFixed(0) + "% of pages have a title"; } },
        { key: "formLabels",     label: "Form Labels",       tip: function (d) { return (d.wcag_form_labels_pct != null ? d.wcag_form_labels_pct : 100).toFixed(0) + "% of form inputs labelled"; } },
        { key: "landmarks",      label: "Landmark Regions",  tip: function (d) { return (d.wcag_landmarks_pct || 0).toFixed(0) + "% of pages have main landmark or skip link"; } },
        { key: "linkPurpose",    label: "Link Purpose",      tip: function (d) { return (100 - (d.wcag_vague_link_pct || 0)).toFixed(0) + "% of links have descriptive text"; } },
        { key: "structuredData", label: "Structured Data", tip: function (d) { return (d.has_json_ld_pct || 0).toFixed(0) + "% JSON-LD, " + (d.has_microdata_pct || 0).toFixed(0) + "% Microdata"; } },
        { key: "metadataCompleteness", label: "Metadata Depth", tip: function (d) { return (d.top_authors && d.top_authors.length ? "Has authors" : "No authors") + ", " + (d.top_publishers && d.top_publishers.length ? "has publisher" : "no publisher"); } },
        { key: "extractionCoverage", label: "Extraction Coverage", tip: function (d) { return (d.avg_extraction_coverage || 0).toFixed(1) + "% avg fields populated"; } }
      ];

      var stableColour = d3.scaleOrdinal(CSJ_PALETTE);

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
  VIZ.wordcloud = function () {
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
  // 10. Sankey — Cross-Domain Journey Flow
  // ────────────────────────────────────────────────────────────────
  VIZ.sankey = function () {
    fetchJSON(API.graph).then(function (data) {
      hideLoading("loading-sankey");
      var topN = parseInt(document.getElementById("sankey-count").value) || 25;

      // Normalise — force simulation may have mutated source/target to objects
      var rawLinks = data.links.map(function (l) {
        return {
          source: l.source.id || l.source,
          target: l.target.id || l.target,
          weight: l.weight
        };
      });

      var topLinks = rawLinks.slice().sort(function (a, b) { return b.weight - a.weight; }).slice(0, topN);
      if (!topLinks.length) return;

      // Separate source / target node pools so the graph is always acyclic
      var srcSeen = {}, tgtSeen = {};
      var sourceIds = [], targetIds = [];
      topLinks.forEach(function (l) {
        if (!srcSeen[l.source]) { sourceIds.push(l.source); srcSeen[l.source] = true; }
        if (!tgtSeen[l.target]) { targetIds.push(l.target); tgtSeen[l.target] = true; }
      });

      var sankeyNodes = [];
      var nodeIdx = {};
      var idx = 0;
      sourceIds.forEach(function (id) {
        sankeyNodes.push({ name: id, side: "source" });
        nodeIdx["from:" + id] = idx++;
      });
      targetIds.forEach(function (id) {
        sankeyNodes.push({ name: id, side: "target" });
        nodeIdx["to:" + id] = idx++;
      });

      var sankeyLinks = topLinks.map(function (l) {
        return {
          source: nodeIdx["from:" + l.source],
          target: nodeIdx["to:" + l.target],
          value: l.weight
        };
      }).filter(function (l) {
        return l.source !== undefined && l.target !== undefined && l.source !== l.target;
      });

      if (!sankeyLinks.length) return;

      var sz = vizSize("viz-sankey");
      var tallestCol = Math.max(sourceIds.length, targetIds.length);
      var W = sz.w, H = Math.max(400, tallestCol * 28);
      var margin = { top: 10, right: 160, bottom: 10, left: 160 };

      d3.select("#viz-sankey").selectAll("*").remove();
      var svg = d3.select("#viz-sankey").append("svg").attr("width", W).attr("height", H);

      var sankey = d3.sankey()
        .nodeWidth(16)
        .nodePadding(12)
        .extent([[margin.left, margin.top], [W - margin.right, H - margin.bottom]]);

      var graph = sankey({
        nodes: sankeyNodes.map(function (d) { return Object.assign({}, d); }),
        links: sankeyLinks.map(function (d) { return Object.assign({}, d); })
      });

      var allDomains = Array.from(new Set(sourceIds.concat(targetIds)));
      var domColour = d3.scaleOrdinal().domain(allDomains).range(CSJ_PALETTE);

      svg.append("g").selectAll("rect").data(graph.nodes).enter().append("rect")
        .attr("x", function (d) { return d.x0; })
        .attr("y", function (d) { return d.y0; })
        .attr("width", function (d) { return d.x1 - d.x0; })
        .attr("height", function (d) { return Math.max(1, d.y1 - d.y0); })
        .attr("fill", function (d) { return domColour(d.name); })
        .on("mouseover", function (evt, d) {
          showTip(evt, "<strong>" + shortDomain(d.name) + "</strong><br>" +
            (d.side === "source" ? "Outgoing links" : "Incoming links"));
        })
        .on("mouseout", hideTip);

      svg.append("g").selectAll("text").data(graph.nodes).enter().append("text")
        .attr("x", function (d) { return d.x0 < W / 2 ? d.x0 - 6 : d.x1 + 6; })
        .attr("y", function (d) { return (d.y0 + d.y1) / 2; })
        .attr("dy", ".35em")
        .attr("text-anchor", function (d) { return d.x0 < W / 2 ? "end" : "start"; })
        .attr("font-size", 10).attr("fill", "#1A1A1A")
        .text(function (d) { return shortDomain(d.name); });

      svg.append("g").attr("fill", "none").selectAll("path").data(graph.links).enter().append("path")
        .attr("d", d3.sankeyLinkHorizontal())
        .attr("stroke", function (d) { return domColour(d.source.name); })
        .attr("stroke-opacity", 0.4)
        .attr("stroke-width", function (d) { return Math.max(1, d.width); })
        .on("mouseover", function (evt, d) {
          d3.select(this).attr("stroke-opacity", 0.7);
          showTip(evt, shortDomain(d.source.name) + " → " + shortDomain(d.target.name) + ": " + fmt(d.value) + " links");
        })
        .on("mouseout", function () { d3.select(this).attr("stroke-opacity", 0.4); hideTip(); });
    });

    document.getElementById("sankey-count").addEventListener("change", function () {
      rendered["sankey"] = false;
      VIZ.sankey();
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 11. Parallel Coordinates
  // ────────────────────────────────────────────────────────────────
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

        var focus = root;
        var view;

        var svg = d3.select("#viz-bubble").append("svg")
          .attr("width", W).attr("height", H)
          .attr("viewBox", [-W / 2, -H / 2, W, H])
          .style("background", "var(--csj-paper)")
          .on("click", function (evt) { zoom(evt, root); });

        var g = svg.append("g");

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

        zoomTo([root.x, root.y, root.r * 2]);
        updateBreadcrumbs(root);

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
  var activeFilters = {};
  var filterOptionsData = null;

  function buildFilterQuery() {
    var parts = [];
    if (activeFilters.cms) parts.push("cms=" + encodeURIComponent(activeFilters.cms));
    if (activeFilters.content_kinds) parts.push("content_kinds=" + encodeURIComponent(activeFilters.content_kinds));
    if (activeFilters.schema_formats) parts.push("schema_formats=" + encodeURIComponent(activeFilters.schema_formats));
    if (activeFilters.min_coverage) parts.push("min_coverage=" + activeFilters.min_coverage);
    return parts.length ? "?" + parts.join("&") : "";
  }

  function addFilterParam(url) {
    var q = buildFilterQuery();
    if (!q) return url;
    return url + (url.indexOf("?") >= 0 ? "&" + q.substring(1) : q);
  }

  var _origFetchJSON = fetchJSON;
  fetchJSON = function (url) {
    var filtered = addFilterParam(url);
    if (cache[filtered]) return Promise.resolve(cache[filtered]);
    return d3.json(filtered).then(function (d) { cache[filtered] = d; return d; });
  };

  function onFilterChange() {
    var cms = document.getElementById("filter-cms").value;
    var kind = document.getElementById("filter-kind").value;
    var schema = document.getElementById("filter-schema").value;
    var cov = document.getElementById("filter-coverage").value;

    activeFilters = {};
    if (cms) activeFilters.cms = cms;
    if (kind) activeFilters.content_kinds = kind;
    if (schema) activeFilters.schema_formats = schema;
    if (cov && parseFloat(cov) > 0) activeFilters.min_coverage = cov;

    var count = Object.keys(activeFilters).length;
    var badge = document.getElementById("filterCount");
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
    var activePanel = document.querySelector(".viz-tab.active");
    if (activePanel) renderPanel(activePanel.dataset.panel);
  }

  function updateFilterChips() {
    var el = document.getElementById("filterChips");
    var chips = [];
    if (activeFilters.cms) chips.push({ key: "cms", label: "CMS: " + activeFilters.cms });
    if (activeFilters.content_kinds) chips.push({ key: "content_kinds", label: "Kind: " + activeFilters.content_kinds });
    if (activeFilters.schema_formats) chips.push({ key: "schema_formats", label: "Schema: " + activeFilters.schema_formats });
    if (activeFilters.min_coverage) chips.push({ key: "min_coverage", label: "Coverage ≥ " + activeFilters.min_coverage + "%" });
    el.innerHTML = chips.map(function (c) {
      return '<span class="filter-chip">' + c.label +
        ' <span class="filter-chip-x" data-key="' + c.key + '">&times;</span></span>';
    }).join("");
    el.querySelectorAll(".filter-chip-x").forEach(function (x) {
      x.addEventListener("click", function () {
        var key = this.dataset.key;
        if (key === "cms") document.getElementById("filter-cms").value = "";
        if (key === "content_kinds") document.getElementById("filter-kind").value = "";
        if (key === "schema_formats") document.getElementById("filter-schema").value = "";
        if (key === "min_coverage") document.getElementById("filter-coverage").value = "";
        onFilterChange();
      });
    });
  }

  document.getElementById("filter-cms").addEventListener("change", onFilterChange);
  document.getElementById("filter-kind").addEventListener("change", onFilterChange);
  document.getElementById("filter-schema").addEventListener("change", onFilterChange);
  document.getElementById("filter-coverage").addEventListener("change", onFilterChange);
  document.getElementById("filterClearAll").addEventListener("click", function () {
    document.getElementById("filter-cms").value = "";
    document.getElementById("filter-kind").value = "";
    document.getElementById("filter-schema").value = "";
    document.getElementById("filter-coverage").value = "";
    onFilterChange();
  });

  d3.json(API.filter_options).then(function (opts) {
    filterOptionsData = opts;
    var cmsSel = document.getElementById("filter-cms");
    (opts.cms_values || []).forEach(function (v) {
      var o = document.createElement("option"); o.value = v; o.text = v; cmsSel.appendChild(o);
    });
    var kindSel = document.getElementById("filter-kind");
    (opts.content_kinds || []).forEach(function (v) {
      var o = document.createElement("option"); o.value = v; o.text = v; kindSel.appendChild(o);
    });
  });


  // ────────────────────────────────────────────────────────────────
  // 14. CMS Landscape (Treemap)
  // ────────────────────────────────────────────────────────────────
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
  VIZ.structureddata = function () {
    fetchJSON(API.domains).then(function (data) {
      hideLoading("loading-structureddata");
      var signals = ["has_json_ld_pct", "has_microdata_pct", "has_rdfa_pct",
        "has_hreflang_pct", "has_feed_pct", "has_pagination_pct",
        "has_breadcrumb_schema_pct", "robots_noindex_pct"];
      var signalLabels = {
        "has_json_ld_pct": "JSON-LD", "has_microdata_pct": "Microdata",
        "has_rdfa_pct": "RDFa", "has_hreflang_pct": "Hreflang",
        "has_feed_pct": "RSS/Atom Feed", "has_pagination_pct": "Pagination",
        "has_breadcrumb_schema_pct": "Breadcrumb Schema", "robots_noindex_pct": "Noindex"
      };

      var top = data.filter(function (d) { return d.page_count >= 2; }).slice(0, 40);
      if (!top.length) return;

      var colourScale = d3.scaleSequential(d3.interpolateBlues).domain([0, 100]);
      var warnColour = d3.scaleSequential(d3.interpolateReds).domain([0, 100]);
      var sz = vizSize("viz-structureddata");

      var labelW = 200, cellSize = 52;
      var margin = { top: 130, right: 20, bottom: 10, left: labelW };
      var gridW = signals.length * cellSize;
      var rowH = 26;
      var gridH = top.length * rowH;
      var W = Math.max(sz.w, margin.left + gridW + margin.right);
      var H = margin.top + gridH + margin.bottom;

      var svg = d3.select("#viz-structureddata").append("svg").attr("width", W).attr("height", H);
      var g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      signals.forEach(function (sig, j) {
        svg.append("text")
          .attr("x", 0).attr("y", 0)
          .attr("transform",
            "translate(" + (margin.left + j * cellSize + cellSize / 2) + "," + (margin.top - 10) + ") rotate(-55)")
          .attr("font-size", 11).attr("text-anchor", "end").attr("fill", "#4c6272")
          .text(signalLabels[sig] || sig);
      });

      top.forEach(function (d, i) {
        var y = i * rowH;
        g.append("text").attr("x", -8).attr("y", y + rowH / 2)
          .attr("text-anchor", "end").attr("dominant-baseline", "central")
          .attr("font-size", 11).attr("fill", "#1A1A1A").text(shortDomain(d.domain));

        signals.forEach(function (sig, j) {
          var val = d[sig] || 0;
          var isWarn = sig === "robots_noindex_pct";
          g.append("rect")
            .attr("x", j * cellSize + 1).attr("y", y + 1)
            .attr("width", cellSize - 2).attr("height", rowH - 2).attr("rx", 3)
            .attr("fill", val > 0 ? (isWarn ? warnColour(Math.min(val, 100)) : colourScale(Math.min(val, 100))) : "#F2EFEB")
            .on("mouseover", function (evt) {
              showTip(evt, "<strong>" + shortDomain(d.domain) + "</strong><br>" +
                (signalLabels[sig] || sig) + ": " + val.toFixed(1) + "% of " + fmt(d.page_count) + " pages");
            })
            .on("mouseout", hideTip);
          if (val > 0) {
            g.append("text").attr("x", j * cellSize + cellSize / 2).attr("y", y + rowH / 2)
              .attr("text-anchor", "middle").attr("dominant-baseline", "central")
              .attr("font-size", 9).attr("fill", val > 50 ? "#fff" : "#333").attr("pointer-events", "none")
              .text(Math.round(val) + "%");
          }
        });
      });

      buildLegend("legend-structureddata", [
        { colour: colourScale(90), label: "High adoption (>75%)" },
        { colour: colourScale(40), label: "Partial adoption" },
        { colour: colourScale(10), label: "Low adoption (<25%)" },
        { colour: "#F2EFEB", label: "Not detected" }
      ]);
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 16. SEO Readiness Scorecard
  // ────────────────────────────────────────────────────────────────
  VIZ.seoreadiness = function () {
    fetchJSON(API.technology).then(function (data) {
      hideLoading("loading-seoreadiness");
      var seo = (data.seo_readiness || []).slice(0, 40);
      if (!seo.length) return;

      var checks = ["has_canonical", "has_structured_data", "has_breadcrumb_schema",
        "has_hreflang", "has_feed", "has_pagination", "has_robots"];
      var checkLabels = {
        "has_canonical": "Canonical", "has_structured_data": "Structured Data",
        "has_breadcrumb_schema": "Breadcrumb", "has_hreflang": "Hreflang",
        "has_feed": "Feed", "has_pagination": "Pagination", "has_robots": "Robots"
      };

      var sz = vizSize("viz-seoreadiness");
      var labelW = 200, cellSize = 48, rowH = 26;
      var margin = { top: 120, right: 50, bottom: 10, left: labelW };
      var gridW = checks.length * cellSize;
      var gridH = seo.length * rowH;
      var W = Math.max(sz.w, margin.left + gridW + margin.right);
      var H = margin.top + gridH + margin.bottom;

      var svg = d3.select("#viz-seoreadiness").append("svg").attr("width", W).attr("height", H);
      var g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      checks.forEach(function (chk, j) {
        svg.append("text")
          .attr("transform",
            "translate(" + (margin.left + j * cellSize + cellSize / 2) + "," + (margin.top - 10) + ") rotate(-55)")
          .attr("font-size", 11).attr("text-anchor", "end").attr("fill", "#4c6272")
          .text(checkLabels[chk] || chk);
      });

      var green = "#2D6A4F", amber = "#C4841D", red = "#A4243B", grey = "#F2EFEB";

      seo.forEach(function (d, i) {
        var y = i * rowH;
        var pc = d.pages || 1;
        g.append("text").attr("x", -8).attr("y", y + rowH / 2)
          .attr("text-anchor", "end").attr("dominant-baseline", "central")
          .attr("font-size", 11).attr("fill", "#1A1A1A").text(shortDomain(d.domain));

        var totalScore = 0;
        checks.forEach(function (chk, j) {
          var val = d[chk] || 0;
          var pct = val / pc * 100;
          totalScore += pct;
          var colour = pct === 0 ? grey : pct >= 80 ? green : pct >= 30 ? amber : red;
          g.append("rect")
            .attr("x", j * cellSize + 1).attr("y", y + 1)
            .attr("width", cellSize - 2).attr("height", rowH - 2).attr("rx", 3)
            .attr("fill", colour).attr("fill-opacity", pct === 0 ? 1 : 0.8)
            .on("mouseover", function (evt) {
              showTip(evt, "<strong>" + shortDomain(d.domain) + "</strong><br>" +
                (checkLabels[chk] || chk) + ": " + val + " / " + pc + " pages (" + pct.toFixed(1) + "%)");
            })
            .on("mouseout", hideTip);
          if (pct > 0) {
            g.append("text").attr("x", j * cellSize + cellSize / 2).attr("y", y + rowH / 2)
              .attr("text-anchor", "middle").attr("dominant-baseline", "central")
              .attr("font-size", 9).attr("fill", "#fff").attr("pointer-events", "none")
              .text(Math.round(pct) + "%");
          }
        });

        var avgScore = Math.round(totalScore / checks.length);
        g.append("text").attr("x", gridW + 12).attr("y", y + rowH / 2)
          .attr("text-anchor", "start").attr("dominant-baseline", "central")
          .attr("font-size", 10).attr("fill", "#5A5246").attr("font-weight", 600)
          .text(avgScore + "%");
      });

      g.append("text").attr("x", gridW + 12).attr("y", -10)
        .attr("text-anchor", "start").attr("font-size", 10)
        .attr("fill", "#5A5246").attr("font-weight", 600).text("Avg");
    });
  };

  // ────────────────────────────────────────────────────────────────
  // 17. Extraction Coverage Histogram
  // ────────────────────────────────────────────────────────────────
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

  // ── Initial render ──────────────────────────────────────────────
  renderPanel("network");

})();
