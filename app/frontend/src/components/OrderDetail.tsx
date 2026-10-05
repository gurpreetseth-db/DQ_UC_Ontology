import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api } from "@/api";
import { fmt } from "@/format";
import { Async, Badge, Card, DataTable, Field } from "./ui";

export function OrderDetail({ orderId }: { orderId: string }) {
  const q = useQuery({ queryKey: ["order", orderId], queryFn: () => api.order(orderId) });
  return (
    <Async q={q}>
      {({ order: o, lines }) => (
        <>
          <Card title="Order" actions={<Badge>{o.order_status}</Badge>}>
            <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
              <Field label="Order ID"><span className="font-mono">{o.order_id}</span></Field>
              <Field label="Customer">
                <Link className="font-mono text-lava hover:underline" to={`/customers?id=${o.customer_id}`}>{o.customer_id}</Link>
              </Field>
              <Field label="Order date">{fmt.date(o.order_date)}</Field>
              <Field label="Est. delivery">{fmt.date(o.estimated_delivery)}</Field>
              <Field label="Channel">{o.channel}</Field>
              <Field label="Region">{o.region_name} · {o.country_code}</Field>
              <Field label="Items">{o.item_count}</Field>
              <Field label="Order total"><span className="font-semibold">{fmt.money2(o.order_total)}</span></Field>
            </div>
          </Card>
          <div className="grid gap-4 md:grid-cols-2">
            <Card title="Invoice">
              {o.invoice_number ? (
                <div className="grid grid-cols-2 gap-3">
                  <Field label="Invoice #"><span className="font-mono">{o.invoice_number}</span></Field>
                  <Field label="Status"><Badge>{o.invoice_status}</Badge>{o.invoice_overdue && <span className="ml-1"><Badge tone="overdue">overdue</Badge></span>}</Field>
                  <Field label="Issued">{fmt.date(o.invoice_date)}</Field>
                  <Field label="Due">{fmt.date(o.invoice_due_date)}</Field>
                  <Field label="Invoice total">{fmt.money2(o.invoice_total)}</Field>
                </div>
              ) : <p className="text-sm text-navy-500">No invoice issued for this order.</p>}
            </Card>
            <Card title="Return">
              {o.has_return ? (
                <div className="grid grid-cols-2 gap-3">
                  <Field label="Return ID"><span className="font-mono">{o.return_id}</span></Field>
                  <Field label="Status"><Badge>{o.return_status}</Badge></Field>
                  <Field label="Reason">{o.return_reason_code?.replace(/_/g, " ")}</Field>
                  <Field label="Returned">{fmt.date(o.return_date)}</Field>
                  <Field label="Refund">{fmt.money2(o.refund_amount)}</Field>
                  <Field label="Faulty batch">{o.faulty_batch_involved ? <Badge tone="cancelled">yes</Badge> : "no"}</Field>
                </div>
              ) : <p className="text-sm text-navy-500">No return raised.</p>}
            </Card>
          </div>
          <Card title={`Line items (${lines.length})`}>
            <DataTable rows={lines} columns={[
              { key: "sku", label: "SKU", render: (r) => <span className="font-mono text-xs">{r.sku}</span> },
              { key: "product_name", label: "Product", render: (r) => (
                <Link className="hover:text-lava hover:underline" to={`/products?id=${r.product_id}`}>{r.product_name}</Link>) },
              { key: "category_name", label: "Category" },
              { key: "faulty_batch", label: "", render: (r) => r.faulty_batch ? <Badge tone="cancelled">faulty batch</Badge> : null },
              { key: "quantity", label: "Qty", align: "right" },
              { key: "unit_price", label: "Unit", align: "right", render: (r) => fmt.money2(r.unit_price) },
              { key: "discount_pct", label: "Disc.", align: "right", render: (r) => fmt.pct(r.discount_pct) },
              { key: "line_total", label: "Total", align: "right", render: (r) => fmt.money2(r.line_total) },
            ]} />
          </Card>
        </>
      )}
    </Async>
  );
}
