import { description } from '../../package'
import { getDirname, path } from '@vuepress/utils'
import { defaultTheme } from '@vuepress/theme-default'
import { registerComponentsPlugin } from '@vuepress/plugin-register-components'

const __dirname = getDirname(import.meta.url)


 export default {
  /**
   * Ref：https://v1.vuepress.vuejs.org/config/#title
   */
  title: 'PreQL/Trilogy',
  /**
   * Ref：https://v1.vuepress.vuejs.org/config/#description
   */
  description: description,

  /**
   * Extra tags to be injected to the page HTML `<head>`
   *
   * ref：https://v1.vuepress.vuejs.org/config/#head
   */
  head: [
    ['meta', { name: 'theme-color', content: '#3eaf7c' }],
    ['meta', { name: 'apple-mobile-web-app-capable', content: 'yes' }],
    ['meta', { name: 'apple-mobile-web-app-status-bar-style', content: 'black' }],
    ['script', {
      async: true,
      src: 'https://www.googletagmanager.com/gtag/js?id=G-KK1Z9YZMR9'
  }],
    ['script', {}, `window.dataLayer = window.dataLayer || [];
function gtag(){dataLayer.push(arguments);}
gtag('js', new Date());

gtag('config', 'G-KK1Z9YZMR9');
`],
  ],

  theme:  defaultTheme({
    repo: 'https://github.com/preqldata/pypreql',
    editLinks: false,
    docsDir: '/docs/website/docs/src',
    editLinkText: 'Edit this page on GitHub',
    lastUpdated: false,
    contributors: false,
    navbar: [
      {
        text: 'Thesis',
        link: '/thesis/',
      },
      {
        text: 'Concepts',
        link: '/concepts/',
      },
      {
        text: 'Installation',
        link: '/installation/'
      },
      {
        text: 'Demo',
        link: '/demo'
      }
    ],
    // sidebar: [{
    //       text: 'Concepts',
    //       link: '/concepts/',
    //       collapsable: false,
    //       children: [
    //         '',
    //         'using-vue',
    //       ]
    //     }
    //   ]
  }),

  /**
   * Apply plugins，ref：https://v1.vuepress.vuejs.org/zh/plugin/
   */
  plugins: [
    registerComponentsPlugin({
      componentsDir: path.resolve(__dirname, './components'),
    }),
    '@vuepress/plugin-back-to-top',
    '@vuepress/plugin-medium-zoom',
  ]
}
