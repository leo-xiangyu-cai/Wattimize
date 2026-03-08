(function initEnergyFlowDiagram(global) {
  const X6Lib = global.X6;
  const dagreLib = global.dagre;
  if (!X6Lib || !dagreLib) {
    throw new Error("EnergyFlowDiagram requires X6 and dagre to be loaded first.");
  }

  const { Graph, Shape } = X6Lib;
  const HTMLShape = Shape && Shape.HTML;
  if (!HTMLShape) {
    throw new Error("EnergyFlowDiagram requires X6 HTML shape support.");
  }

  const EDGE_INACTIVE_COLOR = "#bfd0c7";
  const EDGE_ACTIVE_COLOR = "#2fa27d";
  const EDGE_ACTIVE_GLOW = "#d6f3e8";
  const EDGE_LABEL_BORDER = "#7ea790";
  const EDGE_LABEL_BG = "rgba(248, 255, 251, 0.96)";
  const DEFAULT_PADDING = 24;
  const EDGE_ARROW_PATH = "M 0 -5 L 10 0 L 0 5 z";
  const CARD_SHAPE = "energy-flow-card";

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

  HTMLShape.register({
    shape: CARD_SHAPE,
    width: 156,
    height: 112,
    effect: ["data"],
    html(cell) {
      return renderNodeBody(cell.getData() || {});
    },
  });

  function createLabelConfig(text) {
    const content = String(text ?? "");
    const width = Math.max(76, Math.round(content.length * 8.8 + 24));
    const height = 28;
    return [{
      position: 0.5,
      markup: [
        { tagName: "rect", selector: "body" },
        { tagName: "text", selector: "label" },
      ],
      attrs: {
        body: {
          x: -width / 2,
          y: -height / 2,
          width,
          height,
          rx: 12,
          ry: 12,
          fill: EDGE_LABEL_BG,
          stroke: EDGE_LABEL_BORDER,
          strokeWidth: 1,
        },
        label: {
          ref: "body",
          refX: "50%",
          refY: "50%",
          text: content,
          fill: "#2d4a3a",
          fontSize: 17,
          fontWeight: 700,
          textAnchor: "middle",
          textVerticalAnchor: "middle",
          pointerEvents: "none",
        },
      },
    }];
  }

  function sortByOrder(items) {
    return [...items].sort((a, b) => {
      const ao = Number.isFinite(a.order) ? a.order : 0;
      const bo = Number.isFinite(b.order) ? b.order : 0;
      if (ao !== bo) return ao - bo;
      return String(a.id).localeCompare(String(b.id));
    });
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
      const bucket = classifyHubNode(node);
      groups[bucket].push(node);
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
    const columnGap = 72;
    const topY = 20;
    const switchboardY = Math.round(height * 0.33);
    const batteryY = height - Math.max(maxHeight(groups.batteryLeft), maxHeight(groups.batteryRight)) - 44;

    if (grid) {
      result[grid.id] = {
        x: (width - grid.width) / 2,
        y: topY,
        width: grid.width,
        height: grid.height,
      };
    }

    if (switchboard) {
      result[switchboard.id] = {
        x: (width - switchboard.width) / 2,
        y: switchboardY,
        width: switchboard.width,
        height: switchboard.height,
      };
    }

    if (solar) {
      result[solar.id] = {
        x: outerX,
        y: Math.max(76, switchboardY - 148),
        width: solar.width,
        height: solar.height,
      };
    }

    if (batteryLeft) {
      result[batteryLeft.id] = {
        x: -10,
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
        x: result[batteryLeft.id].x + batteryLeft.width + columnGap,
        y: result[batteryLeft.id].y + (batteryLeft.height - inverterLeft.height) / 2,
        width: inverterLeft.width,
        height: inverterLeft.height,
      };
    }

    if (inverterRight && batteryRight) {
      result[inverterRight.id] = {
        x: result[batteryRight.id].x - columnGap - inverterRight.width,
        y: result[batteryRight.id].y + (batteryRight.height - inverterRight.height) / 2,
        width: inverterRight.width,
        height: inverterRight.height,
      };
    }

    if (load && switchboard) {
      const switchboardCenterY = center(result[switchboard.id]).y;
      result[load.id] = {
        x: width - outerX - load.width,
        y: switchboardCenterY - load.height / 2,
        width: load.width,
        height: load.height,
      };
    }

    if (ev && load) {
      result[ev.id] = {
        x: result[load.id].x,
        y: result[load.id].y + load.height + 54,
        width: ev.width,
        height: ev.height,
      };
    }

    if (groups.fallback.length) {
      const fallback = layoutDagre({
        nodes: groups.fallback,
        edges: spec.edges.filter((edge) => groups.fallback.some((node) => node.id === edge.source || node.id === edge.target)),
        viewport: { width: width * 0.4, height: height * 0.4 },
      });
      Object.keys(fallback).forEach((id) => {
        const item = fallback[id];
        result[id] = {
          x: item.x + width * 0.3,
          y: item.y + height * 0.28,
          width: item.width,
          height: item.height,
        };
      });
    }

    return result;
  }

  function layoutDagre(spec) {
    const graph = new dagreLib.graphlib.Graph();
    graph.setGraph({
      rankdir: spec.rankdir || "TB",
      nodesep: 36,
      ranksep: 52,
      marginx: DEFAULT_PADDING,
      marginy: DEFAULT_PADDING,
    });
    graph.setDefaultEdgeLabel(() => ({}));

    spec.nodes.forEach((node) => {
      graph.setNode(node.id, { width: node.width, height: node.height });
    });
    spec.edges.forEach((edge) => {
      graph.setEdge(edge.source, edge.target);
    });

    dagreLib.layout(graph);
    const result = {};
    graph.nodes().forEach((id) => {
      const node = graph.node(id);
      result[id] = {
        x: node.x - node.width / 2,
        y: node.y - node.height / 2,
        width: node.width,
        height: node.height,
      };
    });
    return result;
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
        sourceAnchor: { name: dx >= 0 ? "right" : "left" },
        targetAnchor: { name: dx >= 0 ? "left" : "right" },
      };
    }

    return {
      sourceAnchor: { name: dy >= 0 ? "bottom" : "top" },
      targetAnchor: { name: dy >= 0 ? "top" : "bottom" },
    };
  }

  function combinedEdgeGeometry(edgeId, positions) {
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

    if (edgeId === "combined-lineGridToSwitchboard") {
      return {
        source: pointOnBox(grid, "bottom"),
        target: pointOnBox(switchboard, "top"),
      };
    }
    if (edgeId === "combined-lineSolarToBattery1") {
      return {
        source: pointOnBox(solar, "bottom"),
        target: pointOnBox(battery1, "top"),
      };
    }
    if (edgeId === "combined-lineSolarToInverter1A") {
      const elbowX = inverter1.x - 24;
      return {
        source: pointOnBox(solar, "right"),
        target: { x: elbowX, y: center(solar).y },
      };
    }
    if (edgeId === "combined-lineSolarToInverter1B") {
      const elbowX = inverter1.x - 24;
      return {
        source: { x: elbowX, y: center(solar).y },
        target: pointOnBox(inverter1, "left"),
        vertices: [{ x: elbowX, y: center(inverter1).y }],
      };
    }
    if (edgeId === "combined-lineBattery1ToInverter1") {
      return {
        source: pointOnBox(battery1, "right"),
        target: pointOnBox(inverter1, "left"),
      };
    }
    if (edgeId === "combined-lineBattery2ToInverter2") {
      return {
        source: pointOnBox(battery2, "left"),
        target: pointOnBox(inverter2, "right"),
      };
    }
    if (edgeId === "combined-lineInverter1ToSwitchboardA") {
      const source = pointOnBox(inverter1, "top", 24);
      return {
        source,
        target: { x: source.x, y: inverterBusY },
      };
    }
    if (edgeId === "combined-lineInverter1ToSwitchboardB") {
      const source = { x: center(inverter1).x + 24, y: inverterBusY };
      const target = pointOnBox(switchboard, "bottom", -52);
      return {
        source,
        target,
        vertices: [{ x: target.x, y: inverterBusY }],
      };
    }
    if (edgeId === "combined-lineInverter2ToSwitchboardA") {
      const source = pointOnBox(inverter2, "top", -24);
      return {
        source,
        target: { x: source.x, y: inverterBusY },
      };
    }
    if (edgeId === "combined-lineInverter2ToSwitchboardB") {
      const source = { x: center(inverter2).x - 24, y: inverterBusY };
      const target = pointOnBox(switchboard, "bottom", 52);
      return {
        source,
        target,
        vertices: [{ x: target.x, y: inverterBusY }],
      };
    }
    if (edgeId === "combined-lineSwitchboardToHomeLoad") {
      return {
        source: pointOnBox(switchboard, "right"),
        target: pointOnBox(load, "left"),
      };
    }
    if (edgeId === "combined-lineSwitchboardToTeslaA") {
      const trunkX = tesla.x - 18;
      return {
        source: pointOnBox(switchboard, "right"),
        target: { x: trunkX, y: center(switchboard).y },
      };
    }
    if (edgeId === "combined-lineSwitchboardToTeslaB") {
      const trunkX = tesla.x - 18;
      return {
        source: { x: trunkX, y: center(switchboard).y },
        target: pointOnBox(tesla, "left"),
        vertices: [{ x: trunkX, y: center(tesla).y }],
      };
    }
    return null;
  }

  function edgeAttrs(active, reverse) {
    const stroke = active ? EDGE_ACTIVE_COLOR : EDGE_INACTIVE_COLOR;
    const marker = active
      ? {
          name: "path",
          args: {
            d: EDGE_ARROW_PATH,
            fill: EDGE_ACTIVE_COLOR,
            stroke: EDGE_ACTIVE_COLOR,
          },
        }
      : null;

    return {
      line: {
        stroke,
        strokeWidth: active ? 3 : 2,
        targetMarker: reverse ? null : marker,
        sourceMarker: reverse ? marker : null,
      },
      outline: {
        stroke: active ? EDGE_ACTIVE_GLOW : "transparent",
        strokeWidth: active ? 8 : 0,
      },
    };
  }

  class EnergyFlowDiagram {
    constructor(options) {
      this.container = options.container;
      this.spec = options.spec;
      this.edgeCellById = new Map();
      this.labelEdgeById = new Map();
      this.graph = new Graph({
        container: this.container,
        grid: false,
        background: false,
        interacting: false,
        panning: false,
        mousewheel: false,
      });
      this.resizeObserver = new ResizeObserver(() => this.fit());
      this.resizeObserver.observe(this.container);
      this.render();
    }

    render() {
      this.edgeCellById.clear();
      this.labelEdgeById.clear();

      const nodes = this.spec.nodes.map((node) => ({
        ...node,
        width: node.width || 156,
        height: node.height || 112,
      }));
      const layoutInput = {
        nodes,
        edges: this.spec.edges,
        viewport: this.spec.viewport || { width: 1040, height: 700 },
        rankdir: this.spec.rankdir,
      };
      const positions = this.spec.layout === "hub" ? layoutHub(layoutInput) : this.spec.layout === "power" ? layoutPower(layoutInput) : layoutDagre(layoutInput);

      const cells = [];
      nodes.forEach((node) => {
        const position = positions[node.id];
        cells.push(this.graph.createNode({
          id: node.id,
          shape: CARD_SHAPE,
          x: position?.x || 0,
          y: position?.y || 0,
          width: node.width,
          height: node.height,
          data: {
            kind: node.kind,
            icon: node.icon,
            title: node.title,
            titleKey: node.titleKey,
            lines: node.lines,
          },
        }));
      });

      this.spec.edges.forEach((edge) => {
        const sourceBox = positions[edge.source];
        const targetBox = positions[edge.target];
        const attrs = edgeAttrs(false, false);
        const geometry = this.spec.layout === "power" ? combinedEdgeGeometry(edge.id, positions) : null;
        const anchors = geometry ? null : anchorForPair(sourceBox, targetBox);
        const edgeCell = this.graph.createEdge({
          id: edge.id,
          source: geometry?.source || { cell: edge.source, anchor: anchors.sourceAnchor },
          target: geometry?.target || { cell: edge.target, anchor: anchors.targetAnchor },
          vertices: geometry?.vertices || [],
          router: geometry ? { name: "normal" } : { name: "orth" },
          connector: { name: "rounded", args: { radius: 14 } },
          attrs,
          zIndex: 0,
        });
        cells.push(edgeCell);
      });

      this.graph.resetCells(cells);
      this.spec.edges.forEach((edge) => {
        const edgeCell = this.graph.getCellById(edge.id);
        this.edgeCellById.set(edge.id, edgeCell);
        if (edge.labelId) this.labelEdgeById.set(edge.labelId, edgeCell);
      });
      this.fit();
    }

    fit() {
      const rect = this.container.getBoundingClientRect();
      if (!rect.width || !rect.height) return;
      this.graph.resize(rect.width, rect.height);
      const fitPadding = this.spec.layout === "power"
        ? {
            top: DEFAULT_PADDING,
            right: 240,
            bottom: DEFAULT_PADDING,
            left: 8,
          }
        : {
            top: DEFAULT_PADDING,
            right: DEFAULT_PADDING,
            bottom: DEFAULT_PADDING,
            left: DEFAULT_PADDING,
          };
      this.graph.zoomToFit({
        padding: fitPadding,
        maxScale: 1,
      });
      if (this.spec.layout !== "power") {
        this.graph.centerContent();
      }
    }

    setEdgeState(edgeId, active, reverse) {
      const edgeCell = this.edgeCellById.get(edgeId);
      if (!edgeCell) return false;
      edgeCell.attr(edgeAttrs(Boolean(active), Boolean(reverse)));
      return true;
    }

    setEdgeLabel(labelId, text, active) {
      const edgeCell = this.labelEdgeById.get(labelId);
      if (!edgeCell) return false;
      if (!active || text === null || text === undefined || text === "-") {
        edgeCell.setLabels([]);
        return true;
      }
      edgeCell.setLabels(createLabelConfig(text));
      return true;
    }

    dispose() {
      this.resizeObserver.disconnect();
      this.graph.dispose();
    }
  }

  global.EnergyFlowDiagram = EnergyFlowDiagram;
})(window);
