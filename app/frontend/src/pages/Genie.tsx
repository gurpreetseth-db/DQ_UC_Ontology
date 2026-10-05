import { useEffect, useRef, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { Code2, Loader2, RotateCcw, Send, Sparkles } from "lucide-react";
import { api, type GenieAnswer } from "@/api";
import { ErrorBox, Page } from "@/components/ui";

type Turn = { question: string; answer?: GenieAnswer; error?: string };

const SUGGESTIONS = [
  "What was total gross revenue by super region in 2026?",
  "Which product categories have the highest return rate?",
  "Show monthly order count for EMEA-West over the last 6 months",
  "Which faulty-batch products have the most returns?",
  "What is the repeat purchase rate by loyalty tier?",
  "Which region has the highest cancellation rate?",
];

export default function Genie() {
  const [turns, setTurns] = useState<Turn[]>([]);
  const [conversationId, setConversationId] = useState<string>();
  const [text, setText] = useState("");
  const bottom = useRef<HTMLDivElement>(null);

  const ask = useMutation({
    mutationFn: (q: string) => api.askGenie(q, conversationId),
    onSuccess: (a) => {
      if (a.conversation_id) setConversationId(a.conversation_id);
      setTurns((t) => t.map((x, i) => (i === t.length - 1 ? { ...x, answer: a } : x)));
    },
    onError: (e: Error) => setTurns((t) => t.map((x, i) => (i === t.length - 1 ? { ...x, error: e.message } : x))),
  });

  useEffect(() => bottom.current?.scrollIntoView({ behavior: "smooth" }), [turns, ask.isPending]);

  const submit = (q: string) => {
    const question = q.trim();
    if (!question || ask.isPending) return;
    setTurns((t) => [...t, { question }]);
    setText("");
    ask.mutate(question);
  };
  const reset = () => { setTurns([]); setConversationId(undefined); };

  return (
    <Page title="Ask Genie" subtitle="Ask questions in plain English. Answers come from the NexusRetail Analytics Genie space over the governed metrics layer."
      actions={turns.length > 0 && <button className="btn-ghost" onClick={reset} disabled={ask.isPending}><RotateCcw size={14} /> New conversation</button>}>
      <div className="flex min-h-[60vh] flex-col rounded-lg border border-oat-300 bg-white shadow-card">
        <div className="flex-1 space-y-5 overflow-y-auto p-5">
          {turns.length === 0 && (
            <div className="flex flex-col items-center gap-4 py-10 text-center">
              <Sparkles className="text-lava" size={28} />
              <p className="max-w-lg text-sm text-navy-500">
                Genie writes and runs SQL against the certified sales, customer and product metrics.
                Follow-up questions keep the conversation context.
              </p>
              <div className="flex max-w-3xl flex-wrap justify-center gap-2">
                {SUGGESTIONS.map((s) => (
                  <button key={s} onClick={() => submit(s)}
                    className="rounded-full border border-oat-300 px-3 py-1.5 text-sm hover:border-lava hover:text-lava">{s}</button>
                ))}
              </div>
            </div>
          )}
          {turns.map((t, i) => (
            <div key={i} className="space-y-2">
              <div className="flex justify-end">
                <div className="max-w-[80%] rounded-2xl rounded-br-sm bg-navy px-4 py-2 text-sm text-white">{t.question}</div>
              </div>
              {t.answer && <Answer a={t.answer} />}
              {t.error && <ErrorBox error={t.error} />}
              {!t.answer && !t.error && (
                <div className="flex items-center gap-2 text-sm text-navy-500">
                  <Loader2 size={14} className="animate-spin" /> Genie is writing and running a query — this usually takes 15–30s…
                </div>
              )}
            </div>
          ))}
          <div ref={bottom} />
        </div>
        <form className="flex gap-2 border-t border-oat-200 p-3" onSubmit={(e) => { e.preventDefault(); submit(text); }}>
          <input className="input flex-1" placeholder={conversationId ? "Ask a follow-up…" : "Ask about orders, revenue, products, regions…"}
            value={text} onChange={(e) => setText(e.target.value)} disabled={ask.isPending} maxLength={2000} />
          <button className="btn-primary" disabled={ask.isPending || !text.trim()}><Send size={14} /> Ask</button>
        </form>
      </div>
    </Page>
  );
}

function Answer({ a }: { a: GenieAnswer }) {
  const [showSql, setShowSql] = useState(false);
  return (
    <div className="max-w-[90%] space-y-3 rounded-2xl rounded-bl-sm border border-oat-300 bg-oat px-4 py-3">
      <Markdownish text={a.answer} />
      {a.table && a.table.rows.length > 0 && (
        <div className="max-h-72 overflow-auto rounded-md border border-oat-300 bg-white">
          <table className="min-w-full divide-y divide-oat-200 text-sm">
            <thead className="sticky top-0 bg-oat"><tr>{a.table.columns.map((c) => <th key={c} className="th">{c}</th>)}</tr></thead>
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
  const n = Number(v);
  // Round long floats Genie returns (e.g. 100005.19999999995) without touching IDs or dates.
  return /^-?\d+\.\d{3,}$/.test(v) && !Number.isNaN(n) ? n.toLocaleString("en-US", { maximumFractionDigits: 2 }) : v;
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
