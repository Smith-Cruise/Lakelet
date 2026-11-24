import { Logo } from "./Logo";

const DOCS_URL = "https://lakelet.dev/";

/** Paper chrome keeps the workspace visually connected to the docs. */
export function TopBar() {
  return (
    <header className="flex h-[60px] shrink-0 items-center gap-2.5 px-4">
      <Logo size={28} />
      <span className="text-[20px] font-bold tracking-tight">Lakelet</span>
      <span className="ml-2 border-l border-line pl-3 text-[12px] text-fg-muted">
        SQL console
      </span>

      <div className="flex-1" />

      <a
        href={DOCS_URL}
        target="_blank"
        rel="noreferrer"
        className="border-b border-line-strong pb-0.5 text-[12px] font-medium text-fg hover:text-link"
      >
        Docs ↗
      </a>
    </header>
  );
}
