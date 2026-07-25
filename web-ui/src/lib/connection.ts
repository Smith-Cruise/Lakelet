export const DEFAULT_PORT = 6060;

const PORT_KEY = "lakelet.port";

// localStorage access can throw outright when site data is blocked, so every
// use is best effort: losing the saved port must never break the app.
export function loadPort(): number {
  let raw: string | null = null;
  try {
    raw = localStorage.getItem(PORT_KEY);
  } catch {
    return DEFAULT_PORT;
  }
  const port = raw === null ? NaN : Number(raw);
  return Number.isInteger(port) && port >= 1 && port <= 65535 ? port : DEFAULT_PORT;
}

export function savePort(port: number): void {
  try {
    localStorage.setItem(PORT_KEY, String(port));
  } catch {
    // Ignore: the port just won't be remembered next time.
  }
}

// Always 127.0.0.1, never `localhost`: the server binds IPv4 loopback only,
// while `localhost` may resolve to ::1 in some browsers.
export function baseUrl(port: number): string {
  return `http://127.0.0.1:${port}`;
}
