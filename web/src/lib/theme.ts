import { useEffect, useState } from "react";

export type ThemeChoice = "system" | "light" | "dark";

const storageKey = "botsight-theme";

export function savedTheme(): ThemeChoice {
  const v = localStorage.getItem(storageKey);
  return v === "light" || v === "dark" ? v : "system";
}

/** "system" follows the OS; an explicit choice wins both ways (see styles.css). */
export function applyTheme(choice: ThemeChoice): void {
  if (choice === "system") {
    document.documentElement.removeAttribute("data-theme");
    localStorage.removeItem(storageKey);
  } else {
    document.documentElement.setAttribute("data-theme", choice);
    localStorage.setItem(storageKey, choice);
  }
}

/** Chart colors by role, read from the CSS variables of the current theme. */
export interface ChartTokens {
  surface: string;
  text: string;
  textSecondary: string;
  muted: string;
  grid: string;
  axis: string;
  series: string[];
  seq: { low: string; mid: string; high: string };
  critical: string;
}

export function chartTokens(): ChartTokens {
  const css = getComputedStyle(document.documentElement);
  const v = (name: string) => css.getPropertyValue(name).trim();
  return {
    surface: v("--surface"),
    text: v("--text-primary"),
    textSecondary: v("--text-secondary"),
    muted: v("--text-muted"),
    grid: v("--grid"),
    axis: v("--axis"),
    series: [1, 2, 3, 4, 5].map((i) => v(`--series-${i}`)),
    seq: { low: v("--seq-100"), mid: v("--seq-450"), high: v("--seq-700") },
    critical: v("--critical"),
  };
}

/** Changes when the effective theme changes, so charts re-render with its colors. */
export function useThemeVersion(): number {
  const [version, setVersion] = useState(0);
  useEffect(() => {
    const bump = () => setVersion((n) => n + 1);
    const media = window.matchMedia?.("(prefers-color-scheme: dark)");
    media?.addEventListener?.("change", bump);
    const observer = new MutationObserver(bump);
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
    return () => {
      media?.removeEventListener?.("change", bump);
      observer.disconnect();
    };
  }, []);
  return version;
}
