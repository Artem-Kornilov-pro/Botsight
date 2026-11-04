import { useSearchParams } from "react-router-dom";

// The period lives in the URL (?range=30d&tz=Europe/Moscow): a link shows a colleague
// the same slice, and every chart of the page reads the same one.

export const presets = [
  { id: "7d", days: 7, label: "7 дней" },
  { id: "30d", days: 30, label: "30 дней" },
  { id: "90d", days: 90, label: "90 дней" },
] as const;

export type PresetId = (typeof presets)[number]["id"];

export interface Period {
  range: PresetId;
  tz: string;
}

export interface PeriodQuery {
  from: string;
  to: string;
  tz: string;
}

export function browserTimeZone(): string {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  } catch {
    return "UTC";
  }
}

/** Today's date YYYY-MM-DD in the time zone. */
export function today(tz: string, now: Date = new Date()): string {
  // en-CA formats as YYYY-MM-DD
  return new Intl.DateTimeFormat("en-CA", { timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit" }).format(now);
}

function addDays(isoDate: string, days: number): string {
  const d = new Date(isoDate + "T12:00:00Z");
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

/** The API takes whole days in tz, `to` inclusive: "30 days" is today and 29 before. */
export function periodQuery(p: Period, now: Date = new Date()): PeriodQuery {
  const days = presets.find((x) => x.id === p.range)?.days ?? 30;
  const to = today(p.tz, now);
  return { from: addDays(to, -(days - 1)), to, tz: p.tz };
}

export function usePeriod(): [Period, (next: Partial<Period>) => void] {
  const [params, setParams] = useSearchParams();
  const raw = params.get("range");
  const range: PresetId = presets.some((x) => x.id === raw) ? (raw as PresetId) : "30d";
  const tz = params.get("tz") || browserTimeZone();
  const set = (next: Partial<Period>) => {
    const merged = new URLSearchParams(params);
    if (next.range) merged.set("range", next.range);
    if (next.tz) merged.set("tz", next.tz);
    setParams(merged, { replace: true });
  };
  return [{ range, tz }, set];
}
