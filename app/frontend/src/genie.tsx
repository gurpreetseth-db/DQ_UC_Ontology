import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { useMutation } from "@tanstack/react-query";
import { api, type GenieAnswer } from "./api";

// Genie conversation state lives above the router so the chat (and any
// in-flight question) survives navigating between pages.
export type Turn = { question: string; answer?: GenieAnswer; error?: string };

type Ctx = {
  open: boolean;
  setOpen: (open: boolean) => void;
  turns: Turn[];
  pending: boolean;
  conversationId?: string;
  ask: (question: string) => void;
  reset: () => void;
};

const GenieCtx = createContext<Ctx | null>(null);
const OPEN_KEY = "genie-panel-open";

export function GenieProvider({ children }: { children: ReactNode }) {
  const [open, setOpenState] = useState(() => localStorage.getItem(OPEN_KEY) !== "0");
  const [turns, setTurns] = useState<Turn[]>([]);
  const [conversationId, setConversationId] = useState<string>();

  const setOpen = (v: boolean) => {
    setOpenState(v);
    localStorage.setItem(OPEN_KEY, v ? "1" : "0");
  };

  const settleLast = (patch: Partial<Turn>) =>
    setTurns((t) => t.map((x, i) => (i === t.length - 1 ? { ...x, ...patch } : x)));

  const mutation = useMutation({
    mutationFn: (q: string) => api.askGenie(q, conversationId),
    onSuccess: (a) => {
      if (a.conversation_id) setConversationId(a.conversation_id);
      settleLast({ answer: a });
    },
    onError: (e: Error) => settleLast({ error: e.message }),
  });

  const ask = (q: string) => {
    const question = q.trim();
    if (!question || mutation.isPending) return;
    setOpen(true);
    setTurns((t) => [...t, { question }]);
    mutation.mutate(question);
  };

  const reset = () => {
    setTurns([]);
    setConversationId(undefined);
  };

  // Expose the panel width to fixed-position overlays (detail drawers) so they
  // open beside the panel instead of covering it.
  useEffect(() => {
    document.documentElement.style.setProperty("--genie-w", open ? "420px" : "0px");
  }, [open]);

  return (
    <GenieCtx.Provider value={{ open, setOpen, turns, pending: mutation.isPending, conversationId, ask, reset }}>
      {children}
    </GenieCtx.Provider>
  );
}

export function useGenie() {
  const ctx = useContext(GenieCtx);
  if (!ctx) throw new Error("useGenie outside GenieProvider");
  return ctx;
}
