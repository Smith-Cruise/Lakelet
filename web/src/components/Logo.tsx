/**
 * The Lakelet mark: a bolt on a volt tile, cut into three rows. The cuts are
 * tile-coloured bars drawn over the bolt, so no mask or id is needed. Same
 * geometry as `docs/src/assets/lakelet-mark.svg`.
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
      <polygon
        points="18.5,3.5 8,18 15,18 13,28.5 24,13.5 17,13.5"
        fill="var(--lk-accent-contrast)"
      />
      <rect y="9" width="32" height="1.5" fill="var(--lk-accent)" />
      <rect y="15" width="32" height="1.5" fill="var(--lk-accent)" />
      <rect y="21" width="32" height="1.5" fill="var(--lk-accent)" />
    </svg>
  );
}
