const full = new Intl.NumberFormat("ru-RU");
const compactFmt = new Intl.NumberFormat("ru-RU", { notation: "compact", maximumFractionDigits: 1 });
const pct = new Intl.NumberFormat("ru-RU", { style: "percent", maximumFractionDigits: 1 });
const rub = new Intl.NumberFormat("ru-RU", { style: "currency", currency: "RUB", maximumFractionDigits: 2 });

/** 1 284 under ten thousand, 12,9 тыс. above: stat tiles and axes. */
export function compact(n: number): string {
  return Math.abs(n) < 10_000 ? full.format(n) : compactFmt.format(n);
}

export function number(n: number): string {
  return full.format(n);
}

export function percent(share: number): string {
  return pct.format(share);
}

export function rubles(n: number | null | undefined): string {
  return n == null ? "—" : rub.format(n);
}

export function ms(n: number): string {
  return n >= 1000 ? `${(n / 1000).toLocaleString("ru-RU", { maximumFractionDigits: 1 })} с` : `${Math.round(n)} мс`;
}

const dayFmt = new Intl.DateTimeFormat("ru-RU", { day: "numeric", month: "short" });

/** "2026-11-04" -> "4 нояб.", read as a calendar day, not shifted by the time zone. */
export function day(iso: string): string {
  const [y, m, d] = iso.split("-").map(Number);
  return dayFmt.format(new Date(Date.UTC(y ?? 1970, (m ?? 1) - 1, d ?? 1, 12)));
}

const timeFmt = new Intl.DateTimeFormat("ru-RU", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
const dateTimeFmt = new Intl.DateTimeFormat("ru-RU", {
  day: "numeric",
  month: "short",
  hour: "2-digit",
  minute: "2-digit",
});

export function time(iso: string): string {
  return timeFmt.format(new Date(iso));
}

export function dateTime(iso: string): string {
  return dateTimeFmt.format(new Date(iso));
}

/** For chart tooltips, which ECharts renders as HTML: data never goes in unescaped. */
export function escapeHtml(s: string): string {
  return s.replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
}
