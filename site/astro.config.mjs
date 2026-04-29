import { defineConfig } from "astro/config";
import mdx from "@astrojs/mdx";
import react from "@astrojs/react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  integrations: [mdx(), react()],
  vite: {
    plugins: [tailwindcss()],
    build: {
      minify: false,
    },
  },
  markdown: {
    shikiConfig: {
      theme: "github-light",
    },
  },
  output: "static",
  server: {
    host: true,
  },
});
