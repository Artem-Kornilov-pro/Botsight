import type { Option } from "../components/Chart";
import { compact, day, escapeHtml, number } from "./format";
import type { ChartTokens } from "./theme";

// Shared chart anatomy (dataviz skill): hairline solid grid, recessive axes, 2px lines,
// bars at most 24px with a 4px rounded data end, a 2px surface gap between stacked
// segments, one tooltip listing every series with the value first.

export interface Series {
  name: string;
  values: number[];
}

export interface Mark {
  index: number; // position on the category axis
  label: string;
}

function axes(t: ChartTokens, categories: string[], right = 16) {
  return {
    grid: { left: 8, right, top: 36, bottom: 4, containLabel: true },
    xAxis: {
      type: "category",
      data: categories,
      boundaryGap: true,
      axisLine: { lineStyle: { color: t.axis } },
      axisTick: { show: false },
      axisLabel: { color: t.muted, formatter: (v: string) => day(v), hideOverlap: true },
    },
    yAxis: {
      type: "value",
      minInterval: 1,
      splitLine: { lineStyle: { color: t.grid, width: 1, type: "solid" } },
      axisLabel: { color: t.muted, formatter: (v: number) => compact(v) },
    },
  };
}

function legend(t: ChartTokens, series: Series[], mark: "line" | "rect") {
  // a legend is always there for two or more series, none for one: the title names it;
  // its key mirrors the mark: a short stroke for lines, a block for bars
  if (series.length < 2) return { show: false };
  return {
    top: 0,
    left: 0,
    icon: "rect",
    itemWidth: 14,
    itemHeight: mark === "line" ? 3 : 10,
    itemGap: 16,
    textStyle: { color: t.textSecondary },
  };
}

/** One tooltip for every series at the hovered day: value strong, name secondary. */
function tooltip(t: ChartTokens, marks: Mark[] = []) {
  return {
    trigger: "axis",
    axisPointer: { type: "line", lineStyle: { color: t.muted, width: 1, type: "solid" } },
    backgroundColor: t.surface,
    borderColor: t.grid,
    textStyle: { color: t.text },
    formatter: (params: Array<{ axisValue: string; seriesName: string; value: number; color: string; dataIndex: number }>) => {
      const first = params[0];
      if (!first) return "";
      const rows = params
        .map(
          (p) =>
            `<div style="display:flex;align-items:center;gap:8px">` +
            `<span style="width:12px;height:2px;background:${p.color};display:inline-block"></span>` +
            `<b>${escapeHtml(number(p.value))}</b>` +
            `<span style="color:${t.textSecondary}">${escapeHtml(p.seriesName)}</span></div>`,
        )
        .join("");
      const notes = marks
        .filter((m) => m.index === first.dataIndex)
        .map((m) => `<div style="color:${t.muted}">${escapeHtml(m.label)}</div>`)
        .join("");
      return `<div style="color:${t.textSecondary};margin-bottom:4px">${escapeHtml(day(first.axisValue))}</div>${rows}${notes}`;
    },
  };
}

/** Vertical hairlines at deploys (bot starts). Only the main chart labels them, at the
 *  top; on small charts the line alone, the tooltip names it. */
function markLines(t: ChartTokens, marks: Mark[], labelled: boolean) {
  if (!marks.length) return undefined;
  return {
    silent: true,
    symbol: "none",
    lineStyle: { color: t.axis, width: 1, type: "solid" },
    label: labelled
      ? { color: t.muted, formatter: (p: { name: string }) => p.name.replace("Запуск бота · ", ""), position: "end", fontSize: 11 }
      : { show: false },
    data: marks.map((m) => ({ xAxis: m.index, name: m.label })),
  };
}

export function lineChart(t: ChartTokens, days: string[], series: Series[], marks: Mark[] = []): Option {
  return {
    animation: false,
    color: t.series,
    // room on the right for the end labels
    ...axes(t, days, series.length <= 4 ? 56 : 16),
    legend: legend(t, series, "line"),
    tooltip: tooltip(t, marks),
    series: series.map((s, i) => ({
      type: "line",
      name: s.name,
      data: s.values,
      showSymbol: false,
      symbolSize: 8,
      lineStyle: { width: 2, cap: "round", join: "round" },
      emphasis: { focus: "none", itemStyle: { borderColor: t.surface, borderWidth: 2 } },
      // a single series gets the 10% wash under its line
      areaStyle: series.length === 1 ? { opacity: 0.1 } : undefined,
      // direct label at the end supplements the legend; never a number on every point
      endLabel: series.length <= 4 ? { show: true, color: t.textSecondary, formatter: (p: { value: number }) => compact(p.value) } : undefined,
      markLine: i === 0 ? markLines(t, marks, true) : undefined,
    })),
  };
}

export function columnChart(t: ChartTokens, days: string[], series: Series[], marks: Mark[] = []): Option {
  const stacked = series.length > 1;
  return {
    animation: false,
    color: t.series,
    ...axes(t, days),
    legend: legend(t, series, "rect"),
    tooltip: { ...tooltip(t, marks), axisPointer: { type: "shadow", shadowStyle: { color: t.grid, opacity: 0.4 } } },
    series: series.map((s, i) => ({
      type: "bar",
      name: s.name,
      data: s.values,
      stack: stacked ? "total" : undefined,
      barMaxWidth: 24,
      // the top segment gets the rounded data end, the baseline stays square;
      // the surface-colored edge is the 2px gap between stacked segments
      itemStyle: {
        borderRadius: i === series.length - 1 ? [4, 4, 0, 0] : 0,
        borderColor: t.surface,
        borderWidth: stacked ? 1 : 0,
      },
      markLine: i === 0 ? markLines(t, marks, false) : undefined,
    })),
  };
}

/** Ranked horizontal bars of one series: funnel steps, sources. One hue, value at the tip. */
export function barChart(
  t: ChartTokens,
  labels: string[],
  values: number[],
  tip: (i: number) => string,
): Option {
  return {
    animation: false,
    grid: { left: 8, right: 96, top: 8, bottom: 4, containLabel: true },
    xAxis: {
      type: "value",
      splitLine: { lineStyle: { color: t.grid, width: 1, type: "solid" } },
      axisLabel: { color: t.muted, formatter: (v: number) => compact(v) },
    },
    yAxis: {
      type: "category",
      data: labels,
      inverse: true, // the first item on top
      axisLine: { lineStyle: { color: t.axis } },
      axisTick: { show: false },
      axisLabel: { color: t.textSecondary, width: 160, overflow: "truncate" },
    },
    tooltip: {
      trigger: "item",
      backgroundColor: t.surface,
      borderColor: t.grid,
      textStyle: { color: t.text },
      formatter: (p: { dataIndex: number; name: string; value: number }) =>
        `<b>${escapeHtml(number(p.value))}</b> <span style="color:${t.textSecondary}">${escapeHtml(p.name)}</span>` +
        `<div style="color:${t.textSecondary}">${escapeHtml(tip(p.dataIndex))}</div>`,
    },
    series: [
      {
        type: "bar",
        data: values,
        barMaxWidth: 24,
        itemStyle: { color: t.series[0], borderRadius: [0, 4, 4, 0] },
        emphasis: { itemStyle: { opacity: 0.85 } },
        label: { show: true, position: "right", color: t.textSecondary, formatter: (p: { dataIndex: number }) => tip(p.dataIndex) },
      },
    ],
  };
}
