import type { ReactNode } from "react";
import * as Tooltip from "@radix-ui/react-tooltip";

/** A hover label in the app's own style; the provider lives in `main.tsx`. */
export function Tip({ label, children }: { label: string; children: ReactNode }) {
  return (
    <Tooltip.Root>
      <Tooltip.Trigger asChild>{children}</Tooltip.Trigger>
      <Tooltip.Portal>
        {/* Above the trigger, as a small card: it must not cover the rows
            under a column header, and a dark block would fight the table. */}
        <Tooltip.Content
          side="top"
          sideOffset={6}
          collisionPadding={8}
          className="z-50 border border-line-strong bg-panel-alt px-2.5 py-1.5 font-mono text-[11px] text-fg shadow-card"
        >
          {label}
        </Tooltip.Content>
      </Tooltip.Portal>
    </Tooltip.Root>
  );
}
