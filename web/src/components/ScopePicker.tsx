import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import * as DropdownMenu from "@radix-ui/react-dropdown-menu";
import { ChevronDown, Database, Folder, type LucideIcon } from "lucide-react";
import { listCatalogs, listSchemas } from "../flight/client";
import { useApp, type EditorTab } from "../store";

const MENU_CLASS =
  "z-50 max-h-80 min-w-48 overflow-auto rounded-lg border border-line bg-panel p-1 text-[13px] shadow-card";
const ITEM_CLASS =
  "flex h-8 cursor-default items-center rounded-md px-2.5 outline-none data-[highlighted]:bg-accent-bg data-[highlighted]:text-accent";

/**
 * The catalog and schema this tab's statements resolve against, as two
 * dropdown chips. They read the same query keys as the explorer, so a list
 * fetched for one is ready for the other.
 */
export function ScopePicker({ tab }: { tab: EditorTab }) {
  const setTabScope = useApp((state) => state.setTabScope);
  const catalogs = useQuery({ queryKey: ["catalogs"], queryFn: listCatalogs });
  const schemas = useQuery({
    queryKey: ["schemas", tab.catalog],
    queryFn: () => listSchemas(tab.catalog),
  });

  // A tab with no schema would resolve names against the server default,
  // which most catalogs do not have; take the catalog's first schema instead.
  useEffect(() => {
    if (tab.schema === undefined && schemas.data && schemas.data.length > 0) {
      setTabScope(tab.id, tab.catalog, schemas.data[0]);
    }
  }, [tab.id, tab.catalog, tab.schema, schemas.data, setTabScope]);

  return (
    <div className="flex items-center gap-1">
      <Chip
        icon={Database}
        label={tab.catalog}
        options={catalogs.data ?? []}
        // A new catalog has its own schemas; the effect above fills in the
        // first one as soon as the list arrives.
        onSelect={(catalog) => setTabScope(tab.id, catalog, undefined)}
      />
      <Chip
        icon={Folder}
        label={tab.schema ?? "select schema"}
        muted={!tab.schema}
        options={schemas.data ?? []}
        onSelect={(schema) => setTabScope(tab.id, tab.catalog, schema)}
      />
    </div>
  );
}

interface ChipProps {
  icon: LucideIcon;
  label: string;
  muted?: boolean;
  options: string[];
  onSelect: (value: string) => void;
}

function Chip({ icon: Icon, label, muted, options, onSelect }: ChipProps) {
  return (
    <DropdownMenu.Root>
      <DropdownMenu.Trigger
        className={`flex h-8 items-center gap-1.5 rounded-md px-2.5 text-[13px] outline-none hover:bg-hover focus-visible:ring-2 focus-visible:ring-accent ${
          muted ? "text-fg-faint" : "text-fg"
        }`}
      >
        <Icon size={15} className="text-fg-muted" />
        {label}
        <ChevronDown size={13} className="text-fg-faint" />
      </DropdownMenu.Trigger>
      <DropdownMenu.Portal>
        <DropdownMenu.Content className={MENU_CLASS} align="start" sideOffset={6}>
          {options.length === 0 ? (
            <div className="px-2.5 py-1.5 text-fg-faint">nothing to choose</div>
          ) : (
            options.map((name) => (
              <DropdownMenu.Item key={name} className={ITEM_CLASS} onSelect={() => onSelect(name)}>
                {name}
              </DropdownMenu.Item>
            ))
          )}
        </DropdownMenu.Content>
      </DropdownMenu.Portal>
    </DropdownMenu.Root>
  );
}
