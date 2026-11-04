import { BarChart, GraphChart, HeatmapChart, LineChart } from "echarts/charts";
import {
  GridComponent,
  LegendComponent,
  MarkLineComponent,
  TooltipComponent,
  VisualMapComponent,
} from "echarts/components";
import * as echarts from "echarts/core";
import { SVGRenderer } from "echarts/renderers";
import { useEffect, useRef } from "react";

import { chartTokens, useThemeVersion, type ChartTokens } from "../lib/theme";

// Only the parts in use: the bundle stays small. SVG renders crisp at any zoom.
echarts.use([
  LineChart,
  BarChart,
  GraphChart,
  HeatmapChart,
  GridComponent,
  LegendComponent,
  MarkLineComponent,
  TooltipComponent,
  VisualMapComponent,
  SVGRenderer,
]);

export type Option = echarts.EChartsCoreOption;

interface Props {
  /** Built from the theme's tokens, so a theme switch re-renders with its colors. */
  option: (t: ChartTokens) => Option;
  height?: number;
  /** Refetch keeps the frame: the previous render stays, dimmed. */
  stale?: boolean;
  label: string;
}

export function Chart({ option, height = 280, stale, label }: Props) {
  const ref = useRef<HTMLDivElement>(null);
  const chart = useRef<echarts.ECharts | null>(null);
  const theme = useThemeVersion();

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const c = echarts.init(el, undefined, { renderer: "svg" });
    chart.current = c;
    const resize = new ResizeObserver(() => c.resize());
    resize.observe(el);
    return () => {
      resize.disconnect();
      c.dispose();
      chart.current = null;
    };
  }, []);

  useEffect(() => {
    chart.current?.setOption(option(chartTokens()), { notMerge: true });
  }, [option, theme]);

  return (
    <div
      ref={ref}
      className={stale ? "chart stale" : "chart"}
      style={{ height }}
      role="img"
      aria-label={label}
    />
  );
}
