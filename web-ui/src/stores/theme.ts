import { create } from "zustand";

const THEME_KEY = "lakelet.theme";

// Best effort: localStorage can throw when site data is blocked.
function initialDark(): boolean {
  let stored: string | null = null;
  try {
    stored = localStorage.getItem(THEME_KEY);
  } catch {
    stored = null;
  }
  if (stored === "dark") return true;
  if (stored === "light") return false;
  return window.matchMedia("(prefers-color-scheme: dark)").matches;
}

function apply(dark: boolean): void {
  document.documentElement.classList.toggle("dark", dark);
}

interface ThemeState {
  dark: boolean;
  toggle: () => void;
}

export const useTheme = create<ThemeState>((set, get) => {
  const dark = initialDark();
  apply(dark);
  return {
    dark,
    toggle: () => {
      const next = !get().dark;
      try {
        localStorage.setItem(THEME_KEY, next ? "dark" : "light");
      } catch {
        // Ignore: the choice just won't persist across reloads.
      }
      apply(next);
      set({ dark: next });
    },
  };
});
