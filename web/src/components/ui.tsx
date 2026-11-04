import { useState, type ReactNode } from "react";

import { errorText } from "../api/client";
import { presets, type Period } from "../lib/period";

/** A chart and its table twin: every value is reachable without hovering. */
export function ChartCard(props: { title: string; sub?: ReactNode; chart: ReactNode; table: ReactNode }) {
  const [view, setView] = useState<"chart" | "table">("chart");
  return (
    <section className="card">
      <div className="card-head">
        <div>
          <h2>{props.title}</h2>
          {props.sub && <div className="card-sub">{props.sub}</div>}
        </div>
        <div className="segmented" role="group" aria-label="Вид">
          <button aria-pressed={view === "chart"} onClick={() => setView("chart")}>
            График
          </button>
          <button aria-pressed={view === "table"} onClick={() => setView("table")}>
            Таблица
          </button>
        </div>
      </div>
      {view === "chart" ? props.chart : <div style={{ maxHeight: 320, overflow: "auto" }}>{props.table}</div>}
    </section>
  );
}

export function StatTile(props: { label: string; value: string; note?: string }) {
  return (
    <div className="tile">
      <div className="tile-label">{props.label}</div>
      <div className="tile-value">{props.value}</div>
      {props.note && <div className="tile-note">{props.note}</div>}
    </div>
  );
}

const zones = ["Europe/Moscow", "Europe/Kaliningrad", "Asia/Yekaterinburg", "Asia/Novosibirsk", "Asia/Vladivostok", "UTC"];

/** One row above the charts, date range first; it scopes everything below. */
export function PeriodFilters(props: { period: Period; onChange: (p: Partial<Period>) => void }) {
  const tzs = zones.includes(props.period.tz) ? zones : [props.period.tz, ...zones];
  return (
    <div className="filters">
      <div className="segmented" role="group" aria-label="Период">
        {presets.map((p) => (
          <button key={p.id} aria-pressed={props.period.range === p.id} onClick={() => props.onChange({ range: p.id })}>
            {p.label}
          </button>
        ))}
      </div>
      <select aria-label="Часовой пояс" value={props.period.tz} onChange={(e) => props.onChange({ tz: e.target.value })}>
        {tzs.map((z) => (
          <option key={z} value={z}>
            {z}
          </option>
        ))}
      </select>
    </div>
  );
}

export function ErrorState({ error }: { error: unknown }) {
  return (
    <div className="card error" role="alert">
      {errorText(error)}
    </div>
  );
}

export function Loading() {
  return <div className="empty">Загрузка…</div>;
}
