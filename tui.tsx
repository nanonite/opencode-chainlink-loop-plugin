// OpenCode 2 loads a configured plugin directory through entrypoint files:
// `index.js`/`index.ts` for the server plugin and `tui.tsx`/`tui.ts` for the
// CLI plugin. This file re-exports the TUI plugin so the checkout can be
// loaded directly, for example:
//
//   "plugins": ["file:///path/to/opencode-chainlink-loop-plugin"]
//
// Run `bun install` before loading the directory.
export { default } from "./src/tui.tsx"
