import { heatCell, layoutGraph, sourceLabel, stateLabel, stepLabel } from "./analytics";
import { plural } from "./format";

describe("scenario graph layout", () => {
  // labbot: entry -> lab -> name -> confirm, with "start over" from name back to the entry
  const graph = {
    nodes: [
      { state: "", in: 30, out: 100 },
      { state: "Form:lab", in: 100, out: 90 },
      { state: "Form:name", in: 90, out: 90 },
      { state: "Form:confirm", in: 60, out: 0 },
      { state: "Orphan", in: 0, out: 5 },
    ],
    edges: [
      { from: "", to: "Form:lab", transitions: 100, users: 100, share: 1 },
      { from: "Form:lab", to: "Form:name", transitions: 90, users: 90, share: 1 },
      { from: "Form:name", to: "Form:confirm", transitions: 60, users: 60, share: 0.67 },
      { from: "Form:name", to: "", transitions: 30, users: 30, share: 0.33 },
      { from: "Orphan", to: "Form:lab", transitions: 5, users: 5, share: 1 },
    ],
  };

  it("puts states in layers by the shortest path from the entry", () => {
    const { nodes } = layoutGraph(graph);
    const layer = Object.fromEntries(nodes.map((n) => [n.state, n.layer]));
    expect(layer).toEqual({ "": 0, "Form:lab": 1, "Form:name": 2, "Form:confirm": 3, Orphan: 4 });
    expect(nodes.find((n) => n.state === "Form:name")!.x).toBe(2 * 220);
  });

  it("marks loops back and scales widths by users", () => {
    const { edges } = layoutGraph(graph);
    const back = edges.filter((e) => e.back).map((e) => `${e.from}->${e.to}`);
    expect(back).toEqual(["Form:name->", "Orphan->Form:lab"]);
    expect(edges[0]!.width).toBe(8); // the heaviest edge
    expect(edges[4]!.width).toBe(1);
  });

  it("orders a layer by traffic", () => {
    const { nodes } = layoutGraph({
      nodes: [
        { state: "", in: 0, out: 10 },
        { state: "A", in: 2, out: 0 },
        { state: "B", in: 8, out: 0 },
      ],
      edges: [
        { from: "", to: "A", transitions: 2, users: 2, share: 0.2 },
        { from: "", to: "B", transitions: 8, users: 8, share: 0.8 },
      ],
    });
    expect(nodes.find((n) => n.state === "B")!.y).toBe(0);
    expect(nodes.find((n) => n.state === "A")!.y).toBe(90);
  });
});

describe("heatmap cell", () => {
  it("interpolates one hue and picks the readable ink", () => {
    const low = heatCell(0, "#cde2fb", "#0d366b");
    const high = heatCell(1, "#cde2fb", "#0d366b");
    expect(low.background).toBe("rgb(205,226,251)");
    expect(low.color).toBe("#0b0b0b");
    expect(high.background).toBe("rgb(13,54,107)");
    expect(high.color).toBe("#ffffff");
    expect(heatCell(7, "#000000", "#ffffff").background).toBe("rgb(255,255,255)"); // clamped
  });
});

describe("labels", () => {
  it("speaks the dashboard's language", () => {
    expect(stepLabel({ kind: "command", value: "start" })).toBe("/start");
    expect(stepLabel({ kind: "state", value: "Form:name" })).toBe("→ Form:name");
    expect(sourceLabel("")).toBe("без метки");
    expect(stateLabel("")).toBe("вне сценария");
    expect(plural(1, "вид", "вида", "видов")).toBe("1 вид");
    expect(plural(3, "вид", "вида", "видов")).toBe("3 вида");
    expect(plural(11, "вид", "вида", "видов")).toBe("11 видов");
  });
});
