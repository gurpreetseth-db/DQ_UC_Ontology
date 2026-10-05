import { useEffect, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { Search } from "lucide-react";
import { api } from "@/api";
import { FilterBar, useFilters } from "@/filters";
import { fmt } from "@/format";
import { Async, Badge, Card, CsvButton, DataTable, Drawer, Page, type Column } from "@/components/ui";
import { OrderDetail } from "@/components/OrderDetail";

const PAGE = 50;

export const ORDER_COLUMNS: Column[] = [
  { key: "order_id", label: "Order", render: (r) => <span className="font-mono text-xs">{r.order_id}</span> },
  { key: "customer_id", label: "Customer", render: (r) => <span className="font-mono text-xs">{r.customer_id}</span> },
  { key: "order_date", label: "Date", render: (r) => fmt.date(r.order_date) },
  { key: "order_status", label: "Status", render: (r) => <Badge>{r.order_status}</Badge> },
  { key: "channel", label: "Channel" },
  { key: "region_name", label: "Region" },
  { key: "item_count", label: "Items", align: "right" },
  { key: "order_total", label: "Total", align: "right", render: (r) => fmt.money2(r.order_total) },
  { key: "invoice_status", label: "Invoice", render: (r) => r.invoice_status
      ? <Badge tone={r.invoice_overdue ? "overdue" : r.invoice_status}>{r.invoice_overdue ? "overdue" : r.invoice_status}</Badge> : "—" },
  { key: "return_status", label: "Return", render: (r) => r.has_return ? <Badge tone="pending">{r.return_status}</Badge> : "—" },
];

export default function Orders() {
  const { filters } = useFilters();
  const [sp, setSp] = useSearchParams();
  const [status, setStatus] = useState("");
  const [text, setText] = useState("");
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(0);
  const openId = sp.get("id");

  // Debounce free-text search; reset paging whenever the result set changes.
  useEffect(() => { const t = setTimeout(() => setSearch(text.trim()), 350); return () => clearTimeout(t); }, [text]);
  useEffect(() => { setPage(0); }, [filters, status, search]);

  const { data: opts } = useQuery({ queryKey: ["filters"], queryFn: api.filters, staleTime: Infinity });
  const q = useQuery({
    queryKey: ["orders", filters, status, search, page],
    queryFn: () => api.orders({ ...filters, status, q: search, limit: PAGE, offset: page * PAGE }),
    placeholderData: keepPreviousData,
  });
  const setOpen = (id: string | null) => setSp((prev) => {
    const n = new URLSearchParams(prev); id ? n.set("id", id) : n.delete("id"); return n;
  });

  return (
    <Page title="Orders" subtitle="Find an order by ID, customer or invoice number, or browse by region, category and date.">
      <FilterBar />
      <Card
        title={q.data ? `${fmt.num(q.data.total)} orders` : "Orders"}
        actions={<CsvButton name="orders.csv" rows={q.data?.rows ?? []} />}
      >
        <div className="mb-3 flex flex-wrap items-center gap-2">
          <div className="relative">
            <Search size={14} className="absolute left-2.5 top-2.5 text-navy-300" />
            <input className="input w-72 pl-8" placeholder="ORD-…, CUST-… or invoice #" value={text}
              onChange={(e) => setText(e.target.value)} />
          </div>
          <select className="input" value={status} onChange={(e) => setStatus(e.target.value)}>
            <option value="">All statuses</option>
            {opts?.statuses.map((s) => <option key={s}>{s}</option>)}
          </select>
          {q.isFetching && !q.isLoading && <span className="text-xs text-navy-300">Updating…</span>}
        </div>
        <Async q={q}>
          {(d) => (
            <>
              <DataTable rows={d.rows} columns={ORDER_COLUMNS} onRowClick={(r) => setOpen(r.order_id)} />
              {d.total > PAGE && (
                <div className="mt-3 flex items-center justify-end gap-2 text-sm">
                  <span className="text-navy-500">
                    {page * PAGE + 1}–{Math.min((page + 1) * PAGE, d.total)} of {fmt.num(d.total)}
                  </span>
                  <button className="btn-ghost" disabled={page === 0} onClick={() => setPage(page - 1)}>Prev</button>
                  <button className="btn-ghost" disabled={(page + 1) * PAGE >= d.total} onClick={() => setPage(page + 1)}>Next</button>
                </div>
              )}
            </>
          )}
        </Async>
      </Card>
      <Drawer open={!!openId} onClose={() => setOpen(null)} title={`Order ${openId ?? ""}`}>
        {openId && <OrderDetail orderId={openId} />}
      </Drawer>
    </Page>
  );
}
