/// <reference types="vite/client" />

declare module "*.pine?raw" {
  const source: string;
  export default source;
}
