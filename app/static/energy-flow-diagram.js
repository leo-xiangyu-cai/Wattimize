(function initEnergyFlowDiagram(global) {
  const EDGE_INACTIVE_COLOR = "#bfd0c7";
  const EDGE_ACTIVE_COLOR = "#2fa27d";
  const EDGE_ACTIVE_GLOW = "rgba(214, 243, 232, 0.92)";
  const EDGE_INACTIVE_GLOW = "rgba(255, 255, 255, 0.7)";
  const EDGE_INACTIVE_DASH = [10, 8];
  const DEFAULT_PADDING = 28;

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function iconMarkup(name) {
    if (name === "solar") {
      return (
        "<svg viewBox='0 0 24 24' focusable='false'>" +
        "<circle cx='12' cy='12' r='4'></circle>" +
        "<path d='M12 2v3'></path>" +
        "<path d='M12 19v3'></path>" +
        "<path d='M2 12h3'></path>" +
        "<path d='M19 12h3'></path>" +
        "</svg>"
      );
    }
    if (name === "grid") {
      return (
        "<svg viewBox='0 0 24 24' focusable='false'>" +
        "<path d='M12 3l7 4v10l-7 4-7-4V7z'></path>" +
        "<path d='M7 9h10'></path>" +
        "<path d='M7 13h10'></path>" +
        "</svg>"
      );
    }
    if (name === "switchboard") {
      return (
        "<svg viewBox='0 0 24 24' focusable='false'>" +
        "<path d='M12 3l9 5v8l-9 5-9-5V8z'></path>" +
        "<path d='M12 8v8'></path>" +
        "<path d='M8 12h8'></path>" +
        "</svg>"
      );
    }
    if (name === "load") {
      return (
        "<svg viewBox='0 0 24 24' focusable='false'>" +
        "<path d='M4 11l8-7 8 7v9H4z'></path>" +
        "<path d='M10 20v-5h4v5'></path>" +
        "</svg>"
      );
    }
    if (name === "tesla" || name === "ev") {
      return (
        "<svg viewBox='0 0 24 24' focusable='false'>" +
        "<path d='M7 4h10l2 4v8l-2 4H7l-2-4V8z'></path>" +
        "<path d='M10 9l4-2-2 5h3l-5 5 2-5H9z'></path>" +
        "</svg>"
      );
    }
    if (name === "battery") {
      return (
        "<svg viewBox='0 0 24 24' focusable='false'>" +
        "<rect x='6' y='6' width='10' height='12' rx='2' ry='2'></rect>" +
        "<rect x='16' y='10' width='2' height='4'></rect>" +
        "</svg>"
      );
    }
    return (
      "<svg viewBox='0 0 24 24' focusable='false'>" +
      "<rect x='5' y='6' width='14' height='12' rx='2' ry='2'></rect>" +
      "<circle cx='9' cy='12' r='1.5'></circle>" +
      "<path d='M13 10h4'></path>" +
      "<path d='M13 14h4'></path>" +
      "</svg>"
    );
  }

  function renderLine(line) {
    if (!line) return "";
    if (line.type === "soc") {
      return (
        "<div class='soc-wrap'>" +
        "<div class='soc-label'><span data-i18n='socLabel'>SOC</span></div>" +
        "<div class='soc-track'>" +
        `<div id='${escapeHtml(line.fillId)}' class='soc-fill'></div>` +
        "<div class='soc-value-inside'>" +
        `<span id='${escapeHtml(line.valueId)}'>-</span>` +
        `<span id='${escapeHtml(line.energyId)}' class='soc-energy-inside'>-</span>` +
        "</div></div></div>"
      );
    }

    const idAttr = line.id ? ` id="${escapeHtml(line.id)}"` : "";
    const classAttr = escapeHtml(line.className || "node-state");
    const i18nAttr = line.i18nKey ? ` data-i18n="${escapeHtml(line.i18nKey)}"` : "";
    const text = line.text === undefined || line.text === null ? "-" : String(line.text);
    return `<p${idAttr} class="${classAttr}"${i18nAttr}>${escapeHtml(text)}</p>`;
  }

  function renderNodeBody(data) {
    const lines = Array.isArray(data.lines) ? data.lines : [];
    const body = lines.map((line) => renderLine(line)).join("");
    const titleKey = data.titleKey ? ` data-i18n="${escapeHtml(data.titleKey)}"` : "";
    return (
      `<article class="efd-node efd-node-${escapeHtml(data.kind || "generic")}">` +
      "<p class='node-title'>" +
      `<span class='inline-icon' aria-hidden='true'>${iconMarkup(data.icon || data.kind)}</span>` +
      `<span${titleKey}>${escapeHtml(data.title || "")}</span>` +
      "</p>" +
      body +
      "</article>"
    );
  }

  function sortByOrder(items) {
    return [...items].sort((a, b) => {
      const ao = Number.isFinite(a.order) ? a.order : 0;
      const bo = Number.isFinite(b.order) ? b.order : 0;
      if (ao !== bo) return ao - bo;
      return String(a.id).localeCompare(String(b.id));
    });
  }

  function totalWidth(items, gap) {
    if (!items.length) return 0;
    return items.reduce((sum, item) => sum + item.width, 0) + (items.length - 1) * gap;
  }

  function totalHeight(items, gap) {
    if (!items.length) return 0;
    return items.reduce((sum, item) => sum + item.height, 0) + (items.length - 1) * gap;
  }

  function maxWidth(items) {
    return items.reduce((max, item) => Math.max(max, item.width), 0);
  }

  function maxHeight(items) {
    return items.reduce((max, item) => Math.max(max, item.height), 0);
  }

  function placeVertical(items, x, startY, gap, out) {
    let y = startY;
    sortByOrder(items).forEach((item) => {
      out[item.id] = { x, y, width: item.width, height: item.height };
      y += item.height + gap;
    });
  }

  function placeHorizontal(items, startX, y, gap, out) {
    let x = startX;
    sortByOrder(items).forEach((item) => {
      out[item.id] = { x, y, width: item.width, height: item.height };
      x += item.width + gap;
    });
  }

  function classifyHubNode(node) {
    if (node.kind === "solar") return "top";
    if (node.kind === "grid") return "left";
    if (node.kind === "load" || node.kind === "ev" || node.kind === "tesla") return "right";
    if (node.kind === "battery") return "bottom";
    if (node.kind === "switchboard" || node.kind === "inverter" || node.kind === "hub") return "center";
    return "extra";
  }

  function layoutHub(spec) {
    const width = spec.viewport.width;
    const height = spec.viewport.height;
    const result = {};
    const nodes = spec.nodes.map((node) => ({ ...node }));
    const groups = { center: [], top: [], left: [], right: [], bottom: [], extra: [] };
    nodes.forEach((node) => {
      groups[classifyHubNode(node)].push(node);
    });

    const centerNodes = groups.center.length ? groups.center : groups.extra.slice(0, 1);
    const centerNode = centerNodes[0];
    if (centerNode) {
      result[centerNode.id] = {
        x: (width - centerNode.width) / 2,
        y: (height - centerNode.height) / 2,
        width: centerNode.width,
        height: centerNode.height,
      };
    }

    placeHorizontal(groups.top, (width - totalWidth(groups.top, 20)) / 2, 28, 20, result);
    placeVertical(groups.left, 28, (height - totalHeight(groups.left, 24)) / 2, 24, result);
    placeVertical(groups.right, width - maxWidth(groups.right) - 28, (height - totalHeight(groups.right, 24)) / 2, 24, result);
    placeHorizontal(groups.bottom, (width - totalWidth(groups.bottom, 20)) / 2, height - maxHeight(groups.bottom) - 28, 20, result);

    groups.extra.filter((node) => node.id !== centerNode?.id).forEach((node, index) => {
      result[node.id] = {
        x: 28 + ((index % 2) * (node.width + 24)),
        y: 28 + (Math.floor(index / 2) * (node.height + 24)),
        width: node.width,
        height: node.height,
      };
    });

    return result;
  }

  function layoutPower(spec) {
    const width = spec.viewport.width;
    const height = spec.viewport.height;
    const result = {};
    const groups = {
      grid: [],
      solar: [],
      switchboard: [],
      load: [],
      ev: [],
      inverterLeft: [],
      inverterRight: [],
      batteryLeft: [],
      batteryRight: [],
      fallback: [],
    };

    spec.nodes.forEach((node, index) => {
      const side = node.side || (index % 2 === 0 ? "left" : "right");
      if (node.kind === "grid") groups.grid.push(node);
      else if (node.kind === "solar") groups.solar.push(node);
      else if (node.kind === "switchboard" || node.kind === "hub") groups.switchboard.push(node);
      else if (node.kind === "load") groups.load.push(node);
      else if (node.kind === "ev" || node.kind === "tesla") groups.ev.push(node);
      else if (node.kind === "inverter") {
        if (side === "right") groups.inverterRight.push(node);
        else groups.inverterLeft.push(node);
      } else if (node.kind === "battery") {
        if (side === "right") groups.batteryRight.push(node);
        else groups.batteryLeft.push(node);
      } else {
        groups.fallback.push(node);
      }
    });

    const switchboard = groups.switchboard[0];
    const grid = groups.grid[0];
    const solar = groups.solar[0];
    const load = groups.load[0];
    const ev = groups.ev[0];
    const batteryLeft = groups.batteryLeft[0];
    const batteryRight = groups.batteryRight[0];
    const inverterLeft = groups.inverterLeft[0];
    const inverterRight = groups.inverterRight[0];

    const outerX = -10;
    const solarX = -174;
    const solarCenterX = solarX + (solar ? solar.width / 2 : 0);
    const columnGap = 72;
    const leftBatteryShiftX = 0;
    const leftInverterOffsetX = 90;
    const rightInverterOffsetX = 90;
    const switchboardOffsetX = 0;
    const topY = 20;
    const switchboardY = Math.round(height * 0.33);
    const batteryY = height - Math.max(maxHeight(groups.batteryLeft), maxHeight(groups.batteryRight)) - 44;

    if (solar) {
      result[solar.id] = {
        x: solarX,
        y: Math.max(76, switchboardY - 148),
        width: solar.width,
        height: solar.height,
      };
    }
    if (batteryLeft) {
      result[batteryLeft.id] = {
        x: solarCenterX - batteryLeft.width / 2 + leftBatteryShiftX,
        y: batteryY,
        width: batteryLeft.width,
        height: batteryLeft.height,
      };
    }
    if (batteryRight) {
      result[batteryRight.id] = {
        x: width - outerX - batteryRight.width,
        y: batteryY,
        width: batteryRight.width,
        height: batteryRight.height,
      };
    }
    if (inverterLeft && batteryLeft) {
      result[inverterLeft.id] = {
        x: result[batteryLeft.id].x + batteryLeft.width + columnGap + leftInverterOffsetX,
        y: result[batteryLeft.id].y + (batteryLeft.height - inverterLeft.height) / 2,
        width: inverterLeft.width,
        height: inverterLeft.height,
      };
    }
    if (inverterRight && batteryRight) {
      result[inverterRight.id] = {
        x: result[batteryRight.id].x - columnGap - rightInverterOffsetX - inverterRight.width,
        y: result[batteryRight.id].y + (batteryRight.height - inverterRight.height) / 2,
        width: inverterRight.width,
        height: inverterRight.height,
      };
    }
    if (switchboard) {
      let switchboardX = ((width - switchboard.width) / 2) + switchboardOffsetX;
      if (result[inverterLeft.id] && result[inverterRight.id]) {
        switchboardX =
          (((center(result[inverterLeft.id]).x + center(result[inverterRight.id]).x) / 2) - (switchboard.width / 2)) +
          switchboardOffsetX;
      }
      result[switchboard.id] = {
        x: switchboardX,
        y: switchboardY,
        width: switchboard.width,
        height: switchboard.height,
      };
    }
    if (grid) {
      const switchboardCenterX = result[switchboard.id].x + (switchboard.width / 2);
      result[grid.id] = {
        x: switchboardCenterX - (grid.width / 2),
        y: topY,
        width: grid.width,
        height: grid.height,
      };
    }
    if (ev && load) {
      const teslaLineOffsetY = 30;
      result[ev.id] = {
        x: width - outerX - ev.width,
        y: (result[switchboard.id].y + (result[switchboard.id].height / 2) + teslaLineOffsetY) - (ev.height / 2),
        width: ev.width,
        height: ev.height,
      };
    }
    if (load) {
      const loadOffsetLeftOfTesla = 104;
      const loadGapAboveTesla = 78;
      const teslaX = ev ? result[ev.id].x : width - outerX - load.width;
      const teslaY = ev ? result[ev.id].y : (result[switchboard.id].y + 40);
      result[load.id] = {
        x: teslaX - loadOffsetLeftOfTesla,
        y: Math.max(topY + 8, teslaY - load.height - loadGapAboveTesla),
        width: load.width,
        height: load.height,
      };
    }

    groups.fallback.forEach((node, index) => {
      result[node.id] = {
        x: 32 + ((index % 2) * (node.width + 24)),
        y: 32 + (Math.floor(index / 2) * (node.height + 24)),
        width: node.width,
        height: node.height,
      };
    });

    return result;
  }

  function center(box) {
    return { x: box.x + box.width / 2, y: box.y + box.height / 2 };
  }

  function pointOnBox(box, side, offset = 0) {
    if (!box) return { x: 0, y: 0 };
    if (side === "left") return { x: box.x, y: box.y + box.height / 2 + offset };
    if (side === "right") return { x: box.x + box.width, y: box.y + box.height / 2 + offset };
    if (side === "top") return { x: box.x + box.width / 2 + offset, y: box.y };
    return { x: box.x + box.width / 2 + offset, y: box.y + box.height };
  }

  function anchorForPair(sourceBox, targetBox) {
    const source = center(sourceBox);
    const target = center(targetBox);
    const dx = target.x - source.x;
    const dy = target.y - source.y;

    if (Math.abs(dx) >= Math.abs(dy)) {
      return {
        sourceSide: dx >= 0 ? "right" : "left",
        targetSide: dx >= 0 ? "left" : "right",
      };
    }

    return {
      sourceSide: dy >= 0 ? "bottom" : "top",
      targetSide: dy >= 0 ? "top" : "bottom",
    };
  }

  function buildOrthogonalPoints(sourceBox, targetBox) {
    const anchors = anchorForPair(sourceBox, targetBox);
    const source = pointOnBox(sourceBox, anchors.sourceSide);
    const target = pointOnBox(targetBox, anchors.targetSide);

    if (anchors.sourceSide === "left" || anchors.sourceSide === "right") {
      const midX = (source.x + target.x) / 2;
      return [source, { x: midX, y: source.y }, { x: midX, y: target.y }, target];
    }

    const midY = (source.y + target.y) / 2;
    return [source, { x: source.x, y: midY }, { x: target.x, y: midY }, target];
  }

  function combinedEdgeGeometry(edgeId, positions, viewport) {
    const solar = positions["combined-solarNode"];
    const battery1 = positions["combined-battery1Node"];
    const inverter1 = positions["combined-inverter1Node"];
    const inverter2 = positions["combined-inverter2Node"];
    const battery2 = positions["combined-battery2Node"];
    const switchboard = positions["combined-switchboardNode"];
    const grid = positions["combined-gridNode"];
    const load = positions["combined-loadNode"];
    const tesla = positions["combined-teslaNode"];
    const switchboardBottomY = switchboard ? switchboard.y + switchboard.height : 0;
    const inverterBusY = switchboardBottomY + 34;
    const switchboardCenter = switchboard ? center(switchboard) : { x: 0, y: 0 };
    const guideInset = 12;

    if (edgeId === "combined-lineGridToSwitchboard") {
      return [pointOnBox(grid, "bottom"), pointOnBox(switchboard, "top")];
    }
    if (edgeId === "combined-lineSwitchboardMeasureLeft") {
      return [{ x: switchboardCenter.x, y: switchboardCenter.y }, { x: guideInset, y: switchboardCenter.y }];
    }
    if (edgeId === "combined-lineSwitchboardMeasureRight") {
      return [{ x: switchboardCenter.x, y: switchboardCenter.y }, { x: viewport.width - guideInset, y: switchboardCenter.y }];
    }
    if (edgeId === "combined-lineSolarToBattery1") {
      return [pointOnBox(solar, "bottom"), pointOnBox(battery1, "top")];
    }
    if (edgeId === "combined-lineSolarToInverter1A") {
      const target = pointOnBox(inverter1, "top", -18);
      return [pointOnBox(solar, "right"), { x: target.x, y: center(solar).y }];
    }
    if (edgeId === "combined-lineSolarToInverter1B") {
      const target = pointOnBox(inverter1, "top", -18);
      return [{ x: target.x, y: center(solar).y }, target];
    }
    if (edgeId === "combined-lineBattery1ToInverter1") {
      return [pointOnBox(battery1, "right"), pointOnBox(inverter1, "left")];
    }
    if (edgeId === "combined-lineBattery2ToInverter2") {
      return [pointOnBox(battery2, "left"), pointOnBox(inverter2, "right")];
    }
    if (edgeId === "combined-lineInverter1ToSwitchboardA") {
      const source = pointOnBox(inverter1, "top", 24);
      return [source, { x: source.x, y: inverterBusY }];
    }
    if (edgeId === "combined-lineInverter1ToSwitchboardB") {
      const source = { x: center(inverter1).x + 24, y: inverterBusY };
      const pairedSource = { x: center(inverter2).x - 24, y: inverterBusY };
      const sharedMidX = (source.x + pairedSource.x) / 2;
      const bridgeGap = 64;
      const target = { x: sharedMidX - (bridgeGap / 2), y: switchboard.y + switchboard.height };
      return [source, { x: target.x, y: inverterBusY }, target];
    }
    if (edgeId === "combined-lineInverter2ToSwitchboardA") {
      const source = pointOnBox(inverter2, "top", -24);
      return [source, { x: source.x, y: inverterBusY }];
    }
    if (edgeId === "combined-lineInverter2ToSwitchboardB") {
      const source = { x: center(inverter2).x - 24, y: inverterBusY };
      const pairedSource = { x: center(inverter1).x + 24, y: inverterBusY };
      const sharedMidX = (source.x + pairedSource.x) / 2;
      const bridgeGap = 64;
      const target = { x: sharedMidX + (bridgeGap / 2), y: switchboard.y + switchboard.height };
      return [source, { x: target.x, y: inverterBusY }, target];
    }
    if (edgeId === "combined-lineSwitchboardToHomeLoad") {
      const source = { x: center(switchboard).x + 42, y: switchboard.y + 34 };
      const target = pointOnBox(load, "bottom", 0);
      return [source, { x: target.x, y: source.y }, target];
    }
    if (edgeId === "combined-lineSwitchboardToTeslaB") {
      return [pointOnBox(switchboard, "right", 30), pointOnBox(tesla, "left", 0)];
    }
    return null;
  }

  function flattenPoints(points) {
    const out = [];
    points.forEach((point) => {
      out.push(point.x, point.y);
    });
    return out;
  }

  function polylineMidpoint(points) {
    if (!Array.isArray(points) || !points.length) return { x: 0, y: 0 };
    if (points.length === 1) return points[0];
    let total = 0;
    for (let i = 1; i < points.length; i += 1) {
      total += Math.hypot(points[i].x - points[i - 1].x, points[i].y - points[i - 1].y);
    }
    let walked = 0;
    const target = total / 2;
    for (let i = 1; i < points.length; i += 1) {
      const a = points[i - 1];
      const b = points[i];
      const seg = Math.hypot(b.x - a.x, b.y - a.y);
      if (walked + seg >= target) {
        const ratio = seg === 0 ? 0 : (target - walked) / seg;
        return {
          x: a.x + ((b.x - a.x) * ratio),
          y: a.y + ((b.y - a.y) * ratio),
        };
      }
      walked += seg;
    }
    return points[points.length - 1];
  }

  function expandBounds(bounds, x, y) {
    bounds.minX = Math.min(bounds.minX, x);
    bounds.maxX = Math.max(bounds.maxX, x);
    bounds.minY = Math.min(bounds.minY, y);
    bounds.maxY = Math.max(bounds.maxY, y);
  }

  function computeContentBounds(nodes, edgesById) {
    const bounds = {
      minX: Number.POSITIVE_INFINITY,
      minY: Number.POSITIVE_INFINITY,
      maxX: Number.NEGATIVE_INFINITY,
      maxY: Number.NEGATIVE_INFINITY,
    };

    nodes.forEach((node) => {
      expandBounds(bounds, node.x, node.y);
      expandBounds(bounds, node.x + node.width, node.y + node.height);
    });

    edgesById.forEach((edgeMeta) => {
      edgeMeta.points.forEach((point) => {
        expandBounds(bounds, point.x, point.y);
      });
    });

    if (!Number.isFinite(bounds.minX)) {
      return { minX: 0, minY: 0, maxX: 1, maxY: 1, width: 1, height: 1 };
    }

    bounds.width = Math.max(1, bounds.maxX - bounds.minX);
    bounds.height = Math.max(1, bounds.maxY - bounds.minY);
    return bounds;
  }

  function drawPolyline(ctx, points, stroke, lineWidth, dashed) {
    if (!Array.isArray(points) || points.length < 2) return;
    ctx.beginPath();
    ctx.setLineDash(Array.isArray(dashed) ? dashed : dashed ? [6, 6] : []);
    ctx.lineJoin = "round";
    ctx.lineCap = "round";
    ctx.lineWidth = lineWidth;
    ctx.strokeStyle = stroke;
    ctx.moveTo(points[0].x, points[0].y);
    for (let i = 1; i < points.length; i += 1) {
      ctx.lineTo(points[i].x, points[i].y);
    }
    ctx.stroke();
  }

  function drawInactiveMidMarker(ctx, points) {
    const marker = polylineMidpoint(points);
    if (!marker) return;
    ctx.save();
    ctx.beginPath();
    ctx.fillStyle = "#f7fbf8";
    ctx.strokeStyle = "#b7c6be";
    ctx.lineWidth = 1.5;
    ctx.arc(marker.x, marker.y, 4, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
    ctx.restore();
  }

  function drawArrowHead(ctx, from, to, fill) {
    const dx = to.x - from.x;
    const dy = to.y - from.y;
    const length = Math.hypot(dx, dy);
    if (!length) return;
    const ux = dx / length;
    const uy = dy / length;
    const size = 10;
    const width = 5;
    const baseX = to.x - (ux * size);
    const baseY = to.y - (uy * size);
    const px = -uy;
    const py = ux;

    ctx.beginPath();
    ctx.moveTo(to.x, to.y);
    ctx.lineTo(baseX + (px * width), baseY + (py * width));
    ctx.lineTo(baseX - (px * width), baseY - (py * width));
    ctx.closePath();
    ctx.fillStyle = fill;
    ctx.fill();
  }

  class EnergyFlowDiagram {
    constructor(options) {
      this.container = options.container;
      this.spec = options.spec;
      this.edgeStateById = new Map();
      this.edgeMetaById = new Map();
      this.labelMetaById = new Map();
      this.lastPositions = {};
      this.lastViewport = null;
      this.contentBounds = { minX: 0, minY: 0, maxX: 1, maxY: 1, width: 1, height: 1 };
      this.scale = 1;
      this.translateX = 0;
      this.translateY = 0;
      this.buildShell();
      this.resizeObserver = new ResizeObserver(() => this.fit());
      this.resizeObserver.observe(this.container);
      this.render();
    }

    buildShell() {
      this.container.innerHTML = "";

      this.scene = document.createElement("div");
      this.scene.className = "efd-scene";

      this.canvas = document.createElement("canvas");
      this.canvas.className = "efd-canvas";

      this.overlay = document.createElement("div");
      this.overlay.className = "efd-overlay";

      this.labelLayer = document.createElement("div");
      this.labelLayer.className = "efd-label-layer";

      this.nodeLayer = document.createElement("div");
      this.nodeLayer.className = "efd-node-layer";

      this.overlay.appendChild(this.labelLayer);
      this.overlay.appendChild(this.nodeLayer);
      this.scene.appendChild(this.canvas);
      this.scene.appendChild(this.overlay);
      this.container.appendChild(this.scene);
      this.ctx = this.canvas.getContext("2d");
    }

    render() {
      this.edgeMetaById.clear();
      this.labelMetaById.clear();

      const nodes = this.spec.nodes.map((node) => ({
        ...node,
        width: node.width || 156,
        height: node.height || 112,
      }));
      const layoutInput = {
        nodes,
        edges: this.spec.edges,
        viewport: this.spec.viewport || { width: 1040, height: 700 },
      };
      const positions = this.spec.layout === "hub" ? layoutHub(layoutInput) : layoutPower(layoutInput);
      this.lastPositions = positions;
      this.lastViewport = layoutInput.viewport;

      this.renderNodes(nodes, positions);
      this.buildEdges(positions, layoutInput.viewport);
      this.contentBounds = computeContentBounds(Object.values(positions), this.edgeMetaById);
      this.fit();
    }

    renderNodes(nodes, positions) {
      this.nodeLayer.innerHTML = "";
      nodes.forEach((node) => {
        const position = positions[node.id];
        const nodeEl = document.createElement("div");
        nodeEl.className = "efd-node-shell";
        nodeEl.style.left = `${position?.x || 0}px`;
        nodeEl.style.top = `${position?.y || 0}px`;
        nodeEl.style.width = `${node.width}px`;
        nodeEl.style.height = `${node.height}px`;
        nodeEl.innerHTML = renderNodeBody({
          kind: node.kind,
          icon: node.icon,
          title: node.title,
          titleKey: node.titleKey,
          lines: node.lines,
        });
        this.nodeLayer.appendChild(nodeEl);
      });
    }

    buildEdges(positions, viewport) {
      this.labelLayer.innerHTML = "";
      this.spec.edges.forEach((edge) => {
        const sourceBox = positions[edge.source];
        const targetBox = positions[edge.target];
        const points = this.spec.layout === "power"
          ? (combinedEdgeGeometry(edge.id, positions, viewport) || buildOrthogonalPoints(sourceBox, targetBox))
          : buildOrthogonalPoints(sourceBox, targetBox);
        const labelPoint = polylineMidpoint(points);
        this.edgeMetaById.set(edge.id, {
          id: edge.id,
          points,
          flattened: flattenPoints(points),
          measurement: Boolean(edge.measurement),
          labelId: edge.labelId || null,
        });
        this.edgeStateById.set(edge.id, {
          active: false,
          reverse: false,
        });

        if (edge.labelId) {
          const labelEl = document.createElement("div");
          labelEl.id = edge.labelId;
          labelEl.className = "flow-line-value";
          labelEl.textContent = "-";
          labelEl.style.left = `${labelPoint.x}px`;
          labelEl.style.top = `${labelPoint.y}px`;
          this.labelLayer.appendChild(labelEl);
          this.labelMetaById.set(edge.labelId, {
            edgeId: edge.id,
            point: labelPoint,
            element: labelEl,
          });
        }
      });
    }

    fit() {
      const rect = this.container.getBoundingClientRect();
      if (!rect.width || !rect.height || !this.ctx) return;

      const dpr = global.devicePixelRatio || 1;
      this.canvas.width = Math.round(rect.width * dpr);
      this.canvas.height = Math.round(rect.height * dpr);
      this.canvas.style.width = `${rect.width}px`;
      this.canvas.style.height = `${rect.height}px`;

      const paddedWidth = this.contentBounds.width + (DEFAULT_PADDING * 2);
      const paddedHeight = this.contentBounds.height + (DEFAULT_PADDING * 2);
      this.scale = Math.min(rect.width / paddedWidth, rect.height / paddedHeight);
      if (!Number.isFinite(this.scale) || this.scale <= 0) this.scale = 1;

      const scaledWidth = paddedWidth * this.scale;
      const scaledHeight = paddedHeight * this.scale;
      const outerOffsetX = (rect.width - scaledWidth) / 2;
      const outerOffsetY = (rect.height - scaledHeight) / 2;

      this.translateX = outerOffsetX + (DEFAULT_PADDING * this.scale) - (this.contentBounds.minX * this.scale);
      this.translateY = outerOffsetY + (DEFAULT_PADDING * this.scale) - (this.contentBounds.minY * this.scale);

      this.overlay.style.transform = `translate(${this.translateX}px, ${this.translateY}px) scale(${this.scale})`;
      this.redraw(dpr);
    }

    redraw(dpr) {
      const ctx = this.ctx;
      if (!ctx) return;
      const width = this.canvas.width / dpr;
      const height = this.canvas.height / dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, width, height);
      ctx.translate(this.translateX, this.translateY);
      ctx.scale(this.scale, this.scale);

      this.spec.edges.forEach((edge) => {
        const meta = this.edgeMetaById.get(edge.id);
        const state = this.edgeStateById.get(edge.id) || { active: false, reverse: false };
        if (!meta) return;

        if (meta.measurement) {
          drawPolyline(ctx, meta.points, "#6b7d74", 1.5, true);
          return;
        }

        if (state.active) {
          drawPolyline(ctx, meta.points, EDGE_ACTIVE_GLOW, 8, false);
        } else {
          drawPolyline(ctx, meta.points, EDGE_INACTIVE_GLOW, 6, false);
        }
        drawPolyline(
          ctx,
          meta.points,
          state.active ? EDGE_ACTIVE_COLOR : EDGE_INACTIVE_COLOR,
          state.active ? 3 : 2,
          state.active ? false : EDGE_INACTIVE_DASH,
        );

        if (!state.active) {
          drawInactiveMidMarker(ctx, meta.points);
        }

        if (state.active && meta.points.length >= 2) {
          if (state.reverse) {
            drawArrowHead(ctx, meta.points[1], meta.points[0], EDGE_ACTIVE_COLOR);
          } else {
            const last = meta.points.length - 1;
            drawArrowHead(ctx, meta.points[last - 1], meta.points[last], EDGE_ACTIVE_COLOR);
          }
        }
      });
    }

    setEdgeState(edgeId, active, reverse) {
      if (!this.edgeMetaById.has(edgeId)) return false;
      this.edgeStateById.set(edgeId, {
        active: Boolean(active),
        reverse: Boolean(reverse),
      });
      this.redraw(global.devicePixelRatio || 1);
      return true;
    }

    setEdgeLabel(labelId, text, active) {
      const meta = this.labelMetaById.get(labelId);
      if (!meta) return false;
      if (!active || text === null || text === undefined || text === "-") {
        meta.element.textContent = "-";
        meta.element.classList.remove("active");
        return true;
      }
      meta.element.textContent = String(text);
      meta.element.classList.add("active");
      return true;
    }

    getNodeMetrics(nodeId) {
      const box = this.lastPositions?.[nodeId];
      if (!box) return null;
      return {
        ...box,
        centerX: center(box).x,
        centerY: center(box).y,
        viewportWidth: this.lastViewport?.width || 0,
        viewportHeight: this.lastViewport?.height || 0,
      };
    }

    dispose() {
      this.resizeObserver.disconnect();
      this.container.innerHTML = "";
    }
  }

  global.EnergyFlowDiagram = EnergyFlowDiagram;
})(window);
