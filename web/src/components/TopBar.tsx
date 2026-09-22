import { Logo } from "./Logo";

const DOCS_URL = "https://lakelet.dev/";

/** Ink band across the top: the mark and wordmark on the left, docs on the right. */
export function TopBar() {
  return (
    <header className="flex h-11 shrink-0 items-center gap-2.5 border-b-2 border-line-strong bg-fg px-3">
      <Logo size={22} />
      <span className="font-mono text-[15px] font-extrabold tracking-tight text-page">Lakelet</span>
      <span className="ml-1 border border-accent px-1.5 py-0.5 font-mono text-[10px] font-bold tracking-[.14em] text-accent">
        CONSOLE
      </span>

      <div className="flex-1" />

      <a
        href={DOCS_URL}
        target="_blank"
        rel="noreferrer"
        className="border-b border-page/30 pb-0.5 font-mono text-[10px] font-bold tracking-[.14em] text-page/60 outline-none hover:border-accent hover:text-accent focus-visible:border-accent focus-visible:text-accent"
      >
        DOCS ↗
      </a>
    </header>
  );
}
