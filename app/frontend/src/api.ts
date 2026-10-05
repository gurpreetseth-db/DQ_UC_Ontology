// Thin typed client for the FastAPI backend. All filters travel as query params.
export type Filters = {
  region?: string;
  super_region?: string;
  category?: string;
  channel?: string;
  start_date?: string;
  end_date?: string;
};

export type Row = Record<string, any>;

function qs(params: Record<string, unknown>): string {
  const sp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== "" && v !== false) sp.set(k, String(v));
  }
  const s = sp.toString();
  return s ? `?${s}` : "";
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init);
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      detail = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail);
    } catch {
      /* non-JSON error body */
    }
    throw new Error(`${res.status}: ${detail}`);
  }
  return res.json();
}

const get = <T>(path: string, params: Record<string, unknown> = {}) => request<T>(path + qs(params));

export type FilterOptions = {
  regions: { region_name: string; super_region: string }[];
  super_regions: string[];
  categories: string[];
  channels: string[];
  statuses: string[];
  min_date: string;
  max_date: string;
};

export type GenieAnswer = {
  configured: boolean;
  conversation_id?: string;
  message_id?: string;
  status?: string;
  answer: string;
  description?: string;
  sql?: string;
  table?: { columns: string[]; rows: (string | null)[][]; row_count?: number } | null;
};

export const api = {
  health: () => get<Row>("/api/health"),
  whoami: () => get<{ email: string }>("/api/whoami"),
  filters: () => get<FilterOptions>("/api/filters"),
  overview: (f: Filters) => get<{ kpis: Row; trend: Row[]; by_status: Row[] }>("/api/overview", f),
  orders: (f: Filters & { status?: string; q?: string; limit: number; offset: number }) =>
    get<{ total: number; rows: Row[] }>("/api/orders", f),
  order: (id: string) => get<{ order: Row; lines: Row[] }>(`/api/orders/${encodeURIComponent(id)}`),
  customers: (q: string) => get<Row[]>("/api/customers", { q }),
  customer: (id: string) =>
    get<{ profile: Row | null; orders: Row[]; top_categories: Row[] }>(`/api/customers/${encodeURIComponent(id)}`),
  products: (p: { q?: string; category?: string; faulty_only?: boolean; sort?: string }) =>
    get<Row[]>("/api/products", p),
  product: (id: string) =>
    get<{ product: Row; return_reasons: Row[]; monthly: Row[] }>(`/api/products/${encodeURIComponent(id)}`),
  sales: (f: Filters) =>
    get<{ by_category: Row[]; by_region: Row[]; matrix: Row[]; trend: Row[]; notes: Record<string, string> }>(
      "/api/sales",
      f,
    ),
  askGenie: (message: string, conversation_id?: string) =>
    request<GenieAnswer>("/api/genie/ask", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message, conversation_id }),
    }),
};
