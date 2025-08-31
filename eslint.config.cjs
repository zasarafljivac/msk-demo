const js = require('@eslint/js');
const tseslint = require('typescript-eslint');
const pluginN = require('eslint-plugin-n');
const simpleImportSort = require('eslint-plugin-simple-import-sort');
const unusedImports = require('eslint-plugin-unused-imports');
const globals = require('globals');

const tsTypeChecked = tseslint.configs.recommendedTypeChecked.map((c) => ({
  ...c,
  files: ['**/*.{ts,tsx}'],
}));
const tsStylisticChecked = tseslint.configs.stylisticTypeChecked.map((c) => ({
  ...c,
  files: ['**/*.{ts,tsx}'],
}));

module.exports = [
  {
    ignores: [
      'eslint.config.cjs',
      '**/node_modules/**',
      '**/dist/**',
      '**/build/**',
      '**/.sst/**',
      '**/.serverless/**',
      '**/cdk.out/**',
      '**/coverage/**',
    ],
  },

  js.configs.recommended,
  ...tsTypeChecked,
  ...tsStylisticChecked,

  {
    settings: {
      node: {
        tryExtensions: ['.ts', '.tsx', '.js', '.mjs', '.cjs', '.json'],
        resolvePaths: [__dirname],
      },
    },
  },

  {
    files: ['**/*.{ts,tsx}'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      parser: tseslint.parser,
      parserOptions: { project: true, tsconfigRootDir: __dirname },
      globals: { ...globals.es2024, ...globals.node },
    },
    plugins: {
      n: pluginN,
      'simple-import-sort': simpleImportSort,
      'unused-imports': unusedImports,
      '@typescript-eslint': tseslint.plugin,
    },
    rules: {
      'n/no-missing-import': 'error',
      'n/no-process-exit': 'off',
      'unused-imports/no-unused-imports': 'error',
      'unused-imports/no-unused-vars': [
        'warn',
        {
          args: 'after-used',
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
        },
      ],
      'simple-import-sort/imports': 'error',
      'simple-import-sort/exports': 'error',
      eqeqeq: ['error', 'smart'],
      curly: ['error', 'all'],
      'no-constant-binary-expression': 'error',
      '@typescript-eslint/consistent-type-imports': ['error', { prefer: 'type-imports' }],
      '@typescript-eslint/no-floating-promises': 'error',
      '@typescript-eslint/no-misused-promises': [
        'error',
        { checksVoidReturn: { attributes: false } },
      ],
      '@typescript-eslint/restrict-template-expressions': [
        'error',
        { allowNumber: true, allowBoolean: true, allowNullish: true },
      ],
    },
  },

  {
    files: ['**/*.{js,cjs,mjs}'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      globals: { ...globals.es2024, ...globals.node },
    },
    plugins: {
      n: pluginN,
      'simple-import-sort': simpleImportSort,
      'unused-imports': unusedImports,
    },
    rules: {
      'n/no-missing-import': 'error',
      'n/no-process-exit': 'off',
      'unused-imports/no-unused-imports': 'error',
      'unused-imports/no-unused-vars': [
        'warn',
        {
          args: 'after-used',
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
        },
      ],
      'simple-import-sort/imports': 'error',
      'simple-import-sort/exports': 'error',
      eqeqeq: ['error', 'smart'],
      curly: ['error', 'all'],
      'no-constant-binary-expression': 'error',
    },
  },
];
