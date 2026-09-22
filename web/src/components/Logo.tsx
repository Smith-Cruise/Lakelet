/**
 * The Lakelet mark: a bold `L` on a volt tile. Purely typographic, so it reads
 * at every size the app uses — down to a 16px favicon, where anything with
 * more detail turns to mush. Same geometry as `docs/src/assets/lakelet-mark.svg`.
 */
export function Logo({ size = 22 }: { size?: number }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 32 32"
      role="img"
      aria-label="Lakelet"
      className="shrink-0"
    >
      <rect width="32" height="32" fill="var(--lk-accent)" />
      <rect x="6" y="5" width="6.5" height="22" fill="var(--lk-accent-contrast)" />
      <rect x="6" y="20.5" width="20" height="6.5" fill="var(--lk-accent-contrast)" />
    </svg>
  );
}
