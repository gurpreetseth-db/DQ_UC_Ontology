import { useEffect, useRef, useState } from "react";
import { Code2, Loader2, PanelRightClose, RotateCcw, Send, Sparkles } from "lucide-react";
import type { GenieAnswer } from "@/api";
import { useGenie } from "@/genie";
import { ErrorBox } from "./ui";

const SUGGESTIONS = [
  "What was total gross revenue by super region in 2026?",
  "Which product categories have the highest return rate?",
  "Show monthly order count for EMEA-West over the last 6 months",
  "Which faulty-batch products have the most returns?",
  "What is the repeat purchase rate by loyalty tier?",
  "Which region has the highest cancellation rate?",
];

/** Right-hand Genie dock. Mounted once in App, so it persists across pages. */
export function GeniePanel() {
  const { open, setOpen, turns, pending, conversationId, ask, reset } = useGenie();
  const [text, setText] = useState("");
  const bottom = useRef<HTMLDivElement>(null);

  // Braces matter: scrollIntoView returns a Promise in current Chrome, and React
  // would call a returned value as the effect's cleanup function and crash.
  useEffect(() => {
    bottom.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [turns, pending]);

  if (!open) return null;

  const submit = (q: string) => {
    if (!q.trim() || pending) return;
    ask(q);
    setText("");
  };

  return (
    <aside className="sticky top-0 flex h-screen w-[420px] shrink-0 flex-col border-l border-oat-300 bg-white">
      <header className="flex items-center justify-between border-b border-oat-200 px-4 py-3">
        <div className="flex items-center gap-2">
          <Sparkles size={16} className="text-lava" />
          <div>
            <div className="text-sm font-semibold">Ask Genie</div>
            <div className="text-[11px] text-navy-500">NexusRetail Analytics · governed metrics</div>
          </div>
        </div>
        <div className="flex items-center gap-1">
          {turns.length > 0 && (
            <button className="btn-ghost h-8 px-2" title="New conversation" onClick={reset} disabled={pending}>
              <RotateCcw size={14} />
            </button>
          )}
          <button className="btn-ghost h-8 px-2" title="Hide Genie" onClick={() => setOpen(false)}>
            <PanelRightClose size={16} />
          </button>
        </div>
      </header>

      <div className="flex-1 space-y-4 overflow-y-auto p-4">
        {turns.length === 0 && (
          <div className="flex flex-col gap-3 pt-2">
            <p className="text-sm text-navy-500">
              Ask in plain English. Genie writes and runs SQL over the certified sales, customer and product
              metrics. The chat stays open while you move between pages, and follow-up questions keep their context.
            </p>
            <div className="flex flex-col gap-1.5">
              {SUGGESTIONS.map((s) => (
                <button key={s} onClick={() => submit(s)}
                  className="rounded-md border border-oat-300 px-3 py-2 text-left text-sm hover:border-lava hover:text-lava">
                  {s}
                </button>
              ))}
            </div>
          </div>
        )}
        {turns.map((t, i) => (
          <div key={i} className="space-y-2">
            <div className="flex justify-end">
              <div className="max-w-[85%] rounded-2xl rounded-br-sm bg-navy px-3.5 py-2 text-sm text-white">{t.question}</div>
            </div>
            {t.answer && <Answer a={t.answer} />}
            {t.error && <ErrorBox error={t.error} />}
            {!t.answer && !t.error && (
              <div className="flex items-center gap-2 text-sm text-navy-500">
                <Loader2 size={14} className="animate-spin" /> Genie is writing and running a query (usually 15–30s)…
              </div>
            )}
          </div>
        ))}
        <div ref={bottom} />
      </div>

      <form className="flex gap-2 border-t border-oat-200 p-3" onSubmit={(e) => { e.preventDefault(); submit(text); }}>
        <input className="input min-w-0 flex-1" maxLength={2000} disabled={pending}
          placeholder={conversationId ? "Ask a follow-up…" : "Ask about orders, revenue, products…"}
          value={text} onChange={(e) => setText(e.target.value)} />
        <button className="btn-primary" disabled={pending || !text.trim()} title="Ask">
          <Send size={14} />
        </button>
      </form>
    </aside>
  );
}

/** Floating launcher shown while the panel is hidden. */
export function GenieLauncher() {
  const { open, setOpen, pending } = useGenie();
  if (open) return null;
  return (
    <button onClick={() => setOpen(true)}
      className="fixed bottom-5 right-5 z-30 flex items-center gap-2 rounded-full bg-lava px-4 py-3 text-sm font-medium text-white shadow-lg hover:bg-lava-700">
      {pending ? <Loader2 size={16} className="animate-spin" /> : <Sparkles size={16} />} Ask Genie
    </button>
  );
}

function Answer({ a }: { a: GenieAnswer }) {
  const [showSql, setShowSql] = useState(false);
  return (
    <div className="space-y-3 rounded-2xl rounded-bl-sm border border-oat-300 bg-oat px-3.5 py-3">
      <Markdownish text={a.answer} />
      {a.table && a.table.rows.length > 0 && (
        <div className="max-h-64 overflow-auto rounded-md border border-oat-300 bg-white">
          <table className="min-w-full divide-y divide-oat-200 text-sm">
            <thead className="sticky top-0 bg-oat">
              <tr>{a.table.columns.map((c) => <th key={c} className="th">{c}</th>)}</tr>
            </thead>
            <tbody className="divide-y divide-oat-200">
              {a.table.rows.map((r, i) => (
                <tr key={i}>{r.map((v, j) => <td key={j} className="td tabular-nums">{formatCell(v)}</td>)}</tr>
              ))}
            </tbody>
          </table>
          {a.table.row_count != null && a.table.row_count > a.table.rows.length && (
            <div className="px-3 py-1.5 text-xs text-navy-500">Showing {a.table.rows.length} of {a.table.row_count} rows</div>
          )}
        </div>
      )}
      {a.sql && (
        <div>
          <button className="flex items-center gap-1 text-xs font-medium text-navy-500 hover:text-lava" onClick={() => setShowSql(!showSql)}>
            <Code2 size={13} /> {showSql ? "Hide" : "Show"} generated SQL
          </button>
          {showSql && <pre className="mt-2 overflow-x-auto rounded-md bg-navy p-3 font-mono text-xs text-oat">{a.sql}</pre>}
        </div>
      )}
    </div>
  );
}

function formatCell(v: string | null) {
  if (v == null) return "—";
  // Decimals (e.g. 100005.19999999995) → grouped, 2dp. Integers are left alone so IDs and years stay intact.
  if (/^-?\d+\.\d+$/.test(v)) {
    return Number(v).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  // Midnight timestamps (month/day buckets) → just the date.
  const midnight = /^(\d{4}-\d{2}-\d{2})T00:00:00(\.000)?Z$/.exec(v);
  return midnight ? midnight[1] : v;
}

/** Minimal rendering for Genie's markdown: paragraphs, **bold** and `code`. */
function Markdownish({ text }: { text: string }) {
  return (
    <div className="space-y-2 text-sm leading-relaxed">
      {text.split(/\n{2,}/).map((para, i) => (
        <p key={i}>
          {para.split(/(\*\*[^*]+\*\*|`[^`]+`)/g).map((part, j) =>
            part.startsWith("**") ? <strong key={j}>{part.slice(2, -2)}</strong>
              : part.startsWith("`") ? <code key={j} className="rounded bg-oat-200 px-1 font-mono text-xs">{part.slice(1, -1)}</code>
                : part)}
        </p>
      ))}
    </div>
  );
}
