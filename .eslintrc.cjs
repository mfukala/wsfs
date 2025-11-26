module.exports = {
  root: true,
  env: {
    es2022: true,
    browser: true,
    node: true,
  },
  parser: "@typescript-eslint/parser",
  parserOptions: {
    ecmaVersion: 2022,
    sourceType: "module",
  },
  plugins: ["@typescript-eslint"],
  extends: ["eslint:recommended", "plugin:@typescript-eslint/recommended"],
  ignorePatterns: ["dist/**", "dist-test/**", "node_modules/**"],
  overrides: [
    {
      files: ["test/**/*.ts"],
      env: { mocha: true, node: true },
    },
    {
      files: ["src/backend/**/*.ts", "test/server.ts"],
      env: { node: true },
    },
    {
      files: ["src/frontend/**/*.ts"],
      env: { browser: true },
    },
  ],
};
