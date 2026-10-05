import { useEffect, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { Search } from "lucide-react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api } from "@/api";
import { fmt } from "@/format";
import { Async, Badge, Card, CsvButton, DataTable, Drawer, Field, Kpi, PALETTE, Page } from "@/components/ui";

export default function Products() {
  const [sp, setSp] = useSearchParams();
  const openId = sp.get("id");
  const [text, setText] = useState("");
  const [search, setSearch] = useState("");
  const [category, setCategory] = useState("");
  const [faulty, setFaulty] = useState(false);
  const [sort, setSort] = useState("revenue");
  useEffect(() => { const t = setTimeout(() => setSearch(text.trim()), 300); return () => clearTimeout(t); }, [text]);

  const { data: opts } = useQuery({ queryKey: ["filters"], queryFn: api.filters, staleTime: Infinity });
  const q = useQuery({
    queryKey: ["products", search, category, faulty, sort],
    queryFn: () => api.products({ q: search, category, faulty_only: faulty, sort }),
    placeholderData: keepPreviousData,
  });
  const setOpen = (id: string | null) => setSp((prev) => {
    const n = new URLSearchParams(prev); id ? n.set("id", id) : n.delete("id"); return n;
  });

  return (
    <Page title="Products" subtitle="Product catalog with price, lifetime sales and return behaviour.">
      <Card title={q.data ? `${q.data.length} products` : "Products"} actions={<CsvButton name="products.csv" rows={q.data ?? []} />}>
        <div className="mb-3 flex flex-wrap items-center gap-2">
          <div className="relative">
            <Search size={14} className="absolute left-2.5 top-2.5 text-navy-300" />
            <input className="input w-72 pl-8" placeholder="Name, SKU, brand or PROD-…" value={text} onChange={(e) => setText(e.target.value)} />
          </div>
          <select className="input" value={category} onChange={(e) => setCategory(e.target.value)}>
            <option value="">All categories</option>
            {opts?.categories.map((c) => <option key={c}>{c}</option>)}
          </select>
          <select className="input" value={sort} onChange={(e) => setSort(e.target.value)}>
            <option value="revenue">Sort: revenue</option>
            <option value="returns">Sort: return rate</option>
            <option value="name">Sort: name</option>
          </select>
          <label className="flex items-center gap-1.5 text-sm">
            <input type="checkbox" className="accent-lava" checked={faulty} onChange={(e) => setFaulty(e.target.checked)} />
            Faulty batch only
          </label>
        </div>
        <Async q={q}>
          {(rows) => (
            <DataTable rows={rows} maxHeight={620} onRowClick={(r) => setOpen(r.product_id)} columns={[
              { key: "sku", label: "SKU", render: (r) => <span className="font-mono text-xs">{r.sku}</span> },
              { key: "product_name", label: "Product", render: (r) => (
                <span className="flex items-center gap-1.5">{r.product_name}{r.faulty_batch && <Badge tone="cancelled">faulty batch</Badge>}{!r.is_active && <Badge>inactive</Badge>}</span>) },
              { key: "brand", label: "Brand" },
              { key: "category_name", label: "Category", render: (r) => `${r.category_name} › ${r.subcategory_name}` },
              { key: "current_price", label: "Price", align: "right", render: (r) => fmt.money2(r.current_price) },
              { key: "units_sold", label: "Units", align: "right", render: (r) => fmt.num(r.units_sold) },
              { key: "gross_revenue", label: "Revenue", align: "right", render: (r) => fmt.money(r.gross_revenue) },
              { key: "return_rate_pct", label: "Return rate", align: "right", render: (r) => (
                <span className={Number(r.return_rate_pct) >= 20 ? "font-semibold text-lava-700" : ""}>{fmt.pct(r.return_rate_pct)}</span>) },
            ]} />
          )}
        </Async>
      </Card>
      <Drawer open={!!openId} onClose={() => setOpen(null)} title="Product details">
        {openId && <ProductDetail productId={openId} />}
      </Drawer>
    </Page>
  );
}

function ProductDetail({ productId }: { productId: string }) {
  const q = useQuery({ queryKey: ["product", productId], queryFn: () => api.product(productId) });
  return (
    <Async q={q}>
      {({ product: p, return_reasons, monthly }) => (
        <>
          <Card title={p.product_name} subtitle={`${p.brand} · ${p.category_name} › ${p.subcategory_name}`}
            actions={p.faulty_batch ? <Badge tone="cancelled">faulty batch</Badge> : <Badge tone="delivered">ok</Badge>}>
            <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
              <Field label="Product ID"><span className="font-mono">{p.product_id}</span></Field>
              <Field label="SKU"><span className="font-mono">{p.sku}</span></Field>
              <Field label="Current price">{fmt.money2(p.current_price)}</Field>
              <Field label="Base price">{fmt.money2(p.base_price)}</Field>
              <Field label="Weight">{p.weight_kg ? `${p.weight_kg} kg` : "—"}</Field>
              <Field label="Active">{p.is_active ? "yes" : "no"}</Field>
              <Field label="Last sold">{fmt.date(p.last_sold_date)}</Field>
            </div>
          </Card>
          <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
            <Kpi label="Units sold" value={fmt.num(p.units_sold)} />
            <Kpi label="Revenue" value={fmt.money(p.gross_revenue)} />
            <Kpi label="Returns" value={fmt.num(p.return_count)} hint={`${fmt.money(p.refund_total)} refunded`} />
            <Kpi label="Return rate" value={fmt.pct(p.return_rate_pct)} tone={Number(p.return_rate_pct) >= 20 ? "bad" : "default"} />
          </div>
          <Card title="Monthly units sold">
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={monthly}>
                <CartesianGrid stroke="#EEEDE9" vertical={false} />
                <XAxis dataKey="month" tickFormatter={fmt.month} fontSize={11} />
                <YAxis fontSize={11} width={32} allowDecimals={false} />
                <Tooltip labelFormatter={fmt.month} />
                <Bar isAnimationActive={false} dataKey="units" fill={PALETTE[1]} radius={[3, 3, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </Card>
          <Card title="Return reasons">
            <DataTable rows={return_reasons} empty="No returns for this product." columns={[
              { key: "return_reason_code", label: "Reason", render: (r) => r.return_reason_code?.replace(/_/g, " ") },
              { key: "return_category", label: "Category" },
              { key: "returns", label: "Returns", align: "right" },
              { key: "refunds", label: "Refunds", align: "right", render: (r) => fmt.money2(r.refunds) },
              { key: "avg_days", label: "Avg days to return", align: "right", render: (r) => r.avg_days == null ? "—"
                : r.avg_days < 0 ? <span title="Return dated before the order — source data-quality issue"><Badge tone="pending">data issue ({r.avg_days})</Badge></span>
                : r.avg_days },
            ]} />
          </Card>
        </>
      )}
    </Async>
  );
}
