import { Droplets, Moon, Sun } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useTheme } from "@/stores/theme";

export function TopBar() {
  const dark = useTheme((state) => state.dark);
  const toggleTheme = useTheme((state) => state.toggle);

  return (
    <header className="flex h-12 shrink-0 items-center gap-3 border-b px-4">
      <div className="flex items-center gap-2">
        <Droplets className="size-5 text-sky-500" />
        <span className="text-sm font-semibold tracking-tight">Lakelet</span>
      </div>
      <div className="flex-1" />
      <Button variant="ghost" size="icon" onClick={toggleTheme} title="Toggle theme">
        {dark ? <Sun /> : <Moon />}
      </Button>
    </header>
  );
}
