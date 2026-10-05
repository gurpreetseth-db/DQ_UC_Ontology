/** @type {import('tailwindcss').Config} */
// Databricks palette: Lava #FF3621 · Navy #0B2026 · Oat #F9F7F4 / #EEEDE9
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: ['"DM Sans"', "ui-sans-serif", "system-ui", "sans-serif"],
        mono: ['"DM Mono"', "ui-monospace", "monospace"],
      },
      colors: {
        lava: { DEFAULT: "#FF3621", 50: "#FFF1EF", 700: "#BD2B1A" },
        navy: { DEFAULT: "#0B2026", 700: "#1B3139", 500: "#4A5D63", 300: "#90A0A5" },
        oat: { DEFAULT: "#F9F7F4", 200: "#EEEDE9", 300: "#E2DFD8" },
      },
      boxShadow: { card: "0 1px 2px rgba(11,32,38,.04), 0 1px 3px rgba(11,32,38,.07)" },
    },
  },
  plugins: [],
};
