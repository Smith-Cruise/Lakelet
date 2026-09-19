import { Moon, Sun, Waves } from "lucide-react";
import { useApp } from "../store";

/** Brand on the left, theme switch on the right; the tab strip below owns everything else. */
export function TopBar() {
  const { theme, setTheme } = useApp();
  const nextTheme = theme === "dark" ? "light" : "dark";

  return (
    <header className="flex h-12 shrink-0 items-center gap-3 border-b border-line bg-panel px-3.5">
      <span className="flex h-6 w-6 items-center justify-center rounded-[7px] bg-accent text-accent-contrast">
        <Waves size={15} strokeWidth={2.25} />
      </span>
      <span className="text-[16px] font-medium tracking-tight">Lakelet</span>

      <div className="flex-1" />

      <button
        type="button"
        aria-label={`Switch to ${nextTheme} theme`}
        onClick={() => setTheme(nextTheme)}
        className="flex h-8 w-8 items-center justify-center rounded-lg text-fg-muted hover:bg-hover hover:text-fg"
      >
        {theme === "dark" ? <Sun size={15} /> : <Moon size={15} />}
      </button>
    </header>
  );
}
