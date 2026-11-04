import { render, screen } from "@testing-library/react";

import { growthText } from "./AI";
import { topReasons } from "./Antifraud";
import { Progress } from "./Broadcasts";

describe("antifraud reasons", () => {
  it("names the two most frequent reasons in words", () => {
    const texts = { fast_steps: "слишком быстрые ответы", synced_timing: "одинаковые интервалы" };
    expect(topReasons({ synced_timing: 3, fast_steps: 9, fresh_accounts: 1 }, texts)).toBe(
      "слишком быстрые ответы (9); одинаковые интервалы (3)",
    );
    expect(topReasons({}, texts)).toBe("");
  });
});

describe("topic growth", () => {
  it("is a signed change with an arrow, not color alone", () => {
    // Intl puts a no-break space before %
    expect(growthText({ growth: 1.5, messages_prev_7d: 10 })).toMatch(/^▲ 50\s%$/);
    expect(growthText({ growth: 0.75, messages_prev_7d: 10 })).toMatch(/^▼ 25\s%$/);
    expect(growthText({ growth: 1.01, messages_prev_7d: 10 })).toBe("без изменений");
    expect(growthText({ growth: 0, messages_prev_7d: 0 })).toBe("новая");
  });
});

describe("broadcast progress", () => {
  it("draws only the non-empty parts and names every part for screen readers", () => {
    const { container } = render(<Progress stats={{ sent: 6, blocked: 1, failed: 0, pending: 3 }} total={10} />);
    expect(container.querySelectorAll("span")).toHaveLength(3);
    expect(screen.getByRole("img").getAttribute("aria-label")).toBe("доставлено 6, заблокировали 1, ошибка 0, в очереди 3");
  });
});
