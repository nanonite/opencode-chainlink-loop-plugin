// OpenCode 2 loads a configured plugin directory through an entrypoint file
// named index.js or index.ts. This file re-exports the built server plugin so
// the checkout can be loaded directly, for example:
//
//   "plugins": ["file:///path/to/opencode-chainlink-loop-plugin"]
//
// Run `bun install` and `bun run build` before loading the directory.
export { default } from "./dist/server.js"
