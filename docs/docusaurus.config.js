// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

const sourceLinkTemplate = 'https://github.com/drifting-in-space/driftdb/blob/{gitRevision}/{path}#{line}'

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'DriftDB',
  tagline: 'A tiny real-time backend for web apps.',
  url: 'https://driftdb.com',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.png',
  
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  plugins: [
    [
      'docusaurus-plugin-typedoc',
      {
        id: 'vanilla-docs',
        out: 'vanilla-api',
        sidebar: {
          categoryLabel: 'Vanilla JS API',
        },
        entryPoints: ['../js-pkg/packages/driftdb/src/index.ts'],
        tsconfig: '../js-pkg/packages/driftdb/tsconfig.json',
        sourceLinkTemplate
      },
    ],
    [
      'docusaurus-plugin-typedoc',
      {
        id: 'react-docs',
        out: 'react-api',
        sidebar: {
          categoryLabel: 'React API',
        },
        entryPoints: ['../js-pkg/packages/driftdb-react/src/index.tsx'],
        tsconfig: '../js-pkg/packages/driftdb-react/tsconfig.json',
        sourceLinkTemplate
      },
    ],
  ],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // editUrl:
          //   'https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/',
        },
        // blog: {
        //   showReadingTime: true,
        //   editUrl:
        //     'https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/',
        // },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'DriftDB',
        logo: {
          alt: 'DriftDB Logo',
          src: 'img/logo.svg',
        },
        items: [
          {
            type: 'doc',
            docId: 'react',
            position: 'left',
            label: 'React Interface',
          },
          {
            type: 'doc',
            docId: 'introduction',
            position: 'left',
            label: 'Introduction',
          },
          {
            type: 'doc',
            docId: 'api',
            position: 'left',
            label: 'API',
          },
          {
            href: 'https://github.com/drifting-in-space/driftdb',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'React Quickstart',
                to: '/docs/react',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Discord',
                href: 'https://discord.gg/N5sEpsuhh9',
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/drifting_corp',
              },
              {
                label: 'GitHub',
                href: 'https://github.com/drifting-in-space/driftdb',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Drifting in Space Corp. Built with Docusaurus.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),

    scripts: [
      { src: 'https://plausible.io/js/plausible.js', defer: true, 'data-domain': 'driftdb.com' }
    ]
};

module.exports = config;
